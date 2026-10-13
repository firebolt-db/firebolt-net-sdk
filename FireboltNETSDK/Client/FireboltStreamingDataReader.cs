#region License Apache 2.0
/* Copyright 2022 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#endregion

using System.Collections;
using System.Data;
using System.Data.Common;
using System.Text;
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;
using FireboltNETSDK.Exception;
using Newtonsoft.Json;

namespace FireboltDotNetSdk.Client
{
    /// <summary>
    /// Represents a data reader to stream data from a Firebolt database. This class cannot be inherited.
    /// </summary>
    public sealed class FireboltStreamingDataReader : FireboltDataReader
    {
        private const string Start = "START";
        private const string Data = "DATA";
        private const string FinishWithErrors = "FINISH_WITH_ERRORS";
        private const string FinishSuccessfully = "FINISH_SUCCESSFULLY";

        // Limits how much of a multi-line JSON error body is read before giving up on parsing it.
        private const int MaxErrorBodyChars = 1024 * 1024;

        // Read through the StreamReader rather than its BaseStream, so nothing it has buffered and no BOM is lost.
        private readonly StreamReader _reader;
        private readonly char[] _buffer = new char[16 * 1024];
        private int _bufferStart;
        private int _bufferEnd;
        private readonly StringBuilder _partialLine = new();
        private readonly int _readTimeoutMillis;
        private readonly CancellationToken _cancellationToken;
        // Cancelled by the caller's token and by Close.
        private readonly CancellationTokenSource _cancellation = new();
        // Released with the stream, so a long-lived caller token does not keep every finished reader's source alive.
        private readonly CancellationTokenRegistration _cancellationRegistration;
        // Bounds one wait for data. Reused across waits and replaced only after it was cancelled.
        private CancellationTokenSource _wait;
        // Held for every read, so Close never disposes the stream underneath one.
        private readonly SemaphoreSlim _readLock = new(1, 1);
        private List<Meta> _metas = new();
        private readonly Queue<List<object?>> _currentRowQueue = new();
        private List<object?>? _currentRow;
        private bool _endOfStream;
        // Set once a read fails, so later reads keep failing instead of looking like the end of the result.
        private System.Exception? _failure;
        // 1 once Close has started; set atomically so concurrent Close calls shut down only once.
        private int _closing;

        public FireboltStreamingDataReader(StreamReader streamReader) : this(streamReader, 0, CancellationToken.None)
        {
            try
            {
                InitializeAsync().GetAwaiter().GetResult();
            }
            catch
            {
                Close();
                throw;
            }
        }

        private FireboltStreamingDataReader(StreamReader reader, int readTimeoutMillis, CancellationToken cancellationToken) :
            base(null, new QueryResult())
        {
            _reader = reader;
            _readTimeoutMillis = readTimeoutMillis;
            _cancellationToken = cancellationToken;
            _cancellationRegistration = cancellationToken.UnsafeRegister(
                static cancellation => ((CancellationTokenSource)cancellation!).Cancel(), _cancellation);
            _wait = CancellationTokenSource.CreateLinkedTokenSource(_cancellation.Token);
        }

        /// <summary>
        /// Creates a reader that has already read the schema and the first rows, so <see cref="HasRows"/> is known.
        /// </summary>
        /// <param name="readTimeoutMillis">Limit for each wait on the server for more data, 0 for none.</param>
        /// <param name="cancellationToken">Cancels the stream at any point while it is being read.</param>
        internal static async Task<FireboltStreamingDataReader> CreateAsync(StreamReader streamReader, int readTimeoutMillis,
            CancellationToken cancellationToken)
        {
            var reader = new FireboltStreamingDataReader(streamReader, readTimeoutMillis, cancellationToken);
            try
            {
                await reader.InitializeAsync().ConfigureAwait(false);
                return reader;
            }
            catch
            {
                await reader.CloseAsync().ConfigureAwait(false);
                throw;
            }
        }

        /// <inheritdoc/>
        public override bool HasRows => _currentRowIndex >= 0 || _currentRowQueue.Count > 0;

        /// <inheritdoc/>
        public override int FieldCount => _metas.Count;

        /// <inheritdoc/>
        public override int Depth => 0;

        /// <inheritdoc/>
        public override int VisibleFieldCount => FieldCount;

        /// <inheritdoc/>
        public override string GetDataTypeName(int ordinal)
        {
            return _metas[ordinal].Type;
        }

        /// <inheritdoc/>
        public override IEnumerator GetEnumerator()
        {
            return new DbEnumerator(this);
        }

        /// <inheritdoc/>
        public override string GetName(int ordinal)
        {
            return _metas[ordinal].Name;
        }

        /// <inheritdoc/>
        public override int GetOrdinal(string name)
        {
            return _metas.FindIndex(m => m.Name == name);
        }

        /// <inheritdoc/>
        public override object GetValue(int ordinal)
        {
            return GetValueSafely(ordinal) ?? throw new InvalidOperationException($"Column ${ordinal} is null");
        }

        /// <inheritdoc/>
        public override DataTable? GetSchemaTable()
        {
            return BuildSchemaTable(_metas, null);
        }

        /// <inheritdoc/>
        public override int GetValues(object[] values)
        {
            List<object?> row = _currentRow ?? new List<object?>();
            int n = Math.Min(row.Count, values.Length);
            for (int i = 0; i < n; i++)
            {
                values[i] = GetValue(i);
            }
            return n;
        }

        /// <inheritdoc/>
        public override bool IsDBNull(int ordinal)
        {
            ValidateRowIndex();
            return _currentRow != null && _currentRow[ordinal] == null;
        }

        /// <inheritdoc/>
        public override bool Read()
        {
            return ReadAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <inheritdoc/>
        public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            if (IsClosing) return false;
            if (_currentRowQueue.Count == 0 && !await FetchRowAsync(cancellationToken).ConfigureAwait(false))
                return false;

            _currentRow = _currentRowQueue.Dequeue();
            _currentRowIndex++;

            return true;
        }

        protected override List<object?>? GetRow(int ordinal)
        {
            if (ordinal < 0 || ordinal > _currentRow?.Count - 1)
            {
                throw new InvalidOperationException($"Column ${ordinal} does not exist");
            }
            return _currentRow;
        }

        protected override ColumnType GetColumnType(int ordinal)
        {
            return ColumnType.Of(TypesConverter.GetFullColumnTypeName(_metas[ordinal]));
        }

        private async Task InitializeAsync()
        {
            await _readLock.WaitAsync(_cancellationToken).ConfigureAwait(false);
            try
            {
                var line = await NextLineAsync(CancellationToken.None).ConfigureAwait(false);
                if (string.IsNullOrWhiteSpace(line)) throw Fail("Failed to read line from stream");

                StreamingJsonStart? start;
                try
                {
                    start = JsonConvert.DeserializeObject<StreamingJsonStart>(line);
                }
                catch (JsonException e)
                {
                    throw await ErrorFromBodyAsync(line, e).ConfigureAwait(false);
                }
                if (start?.MessageType != Start) throw ErrorFromLine(line);
                _metas = start.Meta ?? new List<Meta>();
            }
            finally
            {
                _readLock.Release();
            }

            await FetchRowAsync(CancellationToken.None).ConfigureAwait(false);
        }

        private async Task<bool> FetchRowAsync(CancellationToken cancellationToken)
        {
            await _readLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (_failure != null)
                    throw new FireboltException("The query result cannot be read further because an earlier read failed.", _failure);
                while (!_endOfStream)
                {
                    var line = await NextLineAsync(cancellationToken).ConfigureAwait(false);
                    if (line == null) throw Fail("Stream ended before the server signalled the end of the result");
                    if (string.IsNullOrWhiteSpace(line)) throw Fail("Failed to read line from stream");

                    StreamingJsonData? json;
                    try
                    {
                        json = JsonConvert.DeserializeObject<StreamingJsonData>(line);
                    }
                    catch (JsonException e)
                    {
                        // Also how a stream cut off inside a line shows up, so release it like any other failure.
                        throw Fail("Failed to parse JSON from stream on line: " + line, e);
                    }
                    switch (json?.MessageType)
                    {
                        case Data:
                            if (json.Data == null) throw Fail("Missing data in DATA message on line: " + line);
                            json.Data.ForEach(_currentRowQueue.Enqueue);
                            // An empty DATA message gives the caller nothing to read, so move on to the next one.
                            if (_currentRowQueue.Count > 0) return true;
                            break;
                        case FinishSuccessfully:
                            Finish();
                            break;
                        default:
                            throw ErrorFromLine(line);
                    }
                }
                return false;
            }
            finally
            {
                _readLock.Release();
            }
        }

        private System.Exception ErrorFromLine(string line)
        {
            StreamingJsonFinishError? json;
            try
            {
                json = JsonConvert.DeserializeObject<StreamingJsonFinishError>(line);
            }
            catch (System.Exception e)
            {
                return Fail("Failed to parse JSON from stream on line: " + line, e);
            }
            // A plain JSON error body (no message_type) can still arrive if the server failed before streaming.
            if (json?.MessageType == FinishWithErrors || (json?.MessageType == null && json?.Errors != null))
                return json.Errors != null
                    ? Failed(new FireboltStructuredException(json.Errors))
                    : Fail("Missing errors in " + FinishWithErrors + " message on line: " + line);
            return Fail("Failed to parse JSON from stream. Unexpected messageType: " + json?.MessageType + " in line: " + line);
        }

        /// <summary>
        /// Handles a first line that is not valid JSON on its own, which is how a pretty-printed JSON error body
        /// with a successful status shows up. Such a body is small, so the rest of it is read to parse it whole.
        /// </summary>
        private async Task<System.Exception> ErrorFromBodyAsync(string firstLine, JsonException cause)
        {
            // Read raw chunks rather than lines, so a body without newlines cannot grow past the limit.
            var body = new StringBuilder(firstLine).Append('\n').Append(_buffer, _bufferStart, _bufferEnd - _bufferStart);
            try
            {
                int read;
                while (body.Length <= MaxErrorBodyChars && (read = await ReadChunkAsync(CancellationToken.None).ConfigureAwait(false)) > 0)
                    body.Append(_buffer, 0, read);
            }
            catch (System.Exception e) when (e is not OperationCanceledException)
            {
                // A stalled or broken body says less than the line that could not be parsed.
                return Fail("Failed to parse JSON from stream on line: " + firstLine, cause);
            }
            try
            {
                var errors = JsonConvert.DeserializeObject<JsonErrorQueryResult>(body.ToString())?.Errors;
                if (errors != null) return Failed(new FireboltStructuredException(errors));
            }
            catch (JsonException)
            {
                // Not an error body either; report the original line below.
            }
            return Fail("Failed to parse JSON from stream on line: " + firstLine, cause);
        }

        /// <summary>
        /// Reads the next line. Must be called while holding the read lock.
        /// </summary>
        private async Task<string?> NextLineAsync(CancellationToken cancellationToken)
        {
            while (true)
            {
                var newline = Array.IndexOf(_buffer, '\n', _bufferStart, _bufferEnd - _bufferStart);
                if (newline >= 0)
                {
                    var line = TakeLine(newline - _bufferStart);
                    _bufferStart = newline + 1;
                    return line;
                }

                _partialLine.Append(_buffer, _bufferStart, _bufferEnd - _bufferStart);
                _bufferStart = _bufferEnd = 0;
                var read = await ReadChunkAsync(cancellationToken).ConfigureAwait(false);
                if (read == 0) return _partialLine.Length == 0 ? null : TakeLine(0);
                _bufferEnd = read;
            }
        }

        /// <summary>
        /// Waits for the next data from the server, bounded by the read timeout.
        /// </summary>
        private async Task<int> ReadChunkAsync(CancellationToken cancellationToken)
        {
            var wait = _wait;
            if (_readTimeoutMillis > 0) wait.CancelAfter(_readTimeoutMillis);
            var registration = cancellationToken.UnsafeRegister(
                static source => ((CancellationTokenSource)source!).Cancel(), wait);
            try
            {
                // The token reaches the HTTP stack, which aborts the connection itself when it is cancelled.
                return await _reader.ReadAsync(_buffer.AsMemory(), wait.Token).ConfigureAwait(false);
            }
            catch (System.Exception e)
            {
                throw ReadFailure(e, cancellationToken, timedOut: wait.IsCancellationRequested);
            }
            finally
            {
                registration.Dispose();
                // TryReset fails once the source was cancelled, including by a timer that fired just after the data
                // arrived; a fresh source keeps that from cancelling the next wait.
                if (!wait.TryReset())
                {
                    wait.Dispose();
                    _wait = CancellationTokenSource.CreateLinkedTokenSource(_cancellation.Token);
                }
            }
        }

        private string TakeLine(int length)
        {
            string line;
            if (_partialLine.Length == 0)
            {
                line = new string(_buffer, _bufferStart, length);
            }
            else
            {
                _partialLine.Append(_buffer, _bufferStart, length);
                line = _partialLine.ToString();
                _partialLine.Clear();
            }
            return line.EndsWith('\r') ? line[..^1] : line;
        }

        private System.Exception ReadFailure(System.Exception cause, CancellationToken cancellationToken, bool timedOut)
        {
            if (IsClosing)
                return Failed(new OperationCanceledException("The reader was closed while reading.", cause));
            if (cancellationToken.IsCancellationRequested)
                return Failed(new OperationCanceledException("The streamed query was cancelled.", cause, cancellationToken));
            if (_cancellationToken.IsCancellationRequested)
                return Failed(new OperationCanceledException("The streamed query was cancelled.", cause, _cancellationToken));
            if (timedOut)
                return Failed(new FireboltTimeoutException(_readTimeoutMillis));
            return Fail("Failed to read the query result from the server.", cause);
        }

        private bool IsClosing => Volatile.Read(ref _closing) == 1;

        private FireboltException Fail(string message, System.Exception? cause = null)
        {
            return Failed(cause == null ? new FireboltException(message) : new FireboltException(message, cause));
        }

        private T Failed<T>(T failure) where T : System.Exception
        {
            _failure = failure;
            ReleaseStream();
            return failure;
        }

        private void Finish()
        {
            _endOfStream = true;
            ReleaseStream();
        }

        private void ReleaseStream()
        {
            // Ending the response right away returns the connection even if the caller never disposes the reader.
            _reader.Dispose();
            _cancellationRegistration.Dispose();
        }

        /// <inheritdoc/>
        public override void Close()
        {
            if (!BeginClose()) return;
            _readLock.Wait(CancellationToken.None);
            EndClose();
        }

        /// <inheritdoc/>
        public override async Task CloseAsync()
        {
            if (!BeginClose()) return;
            await _readLock.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            EndClose();
        }

        /// <inheritdoc/>
        public override async ValueTask DisposeAsync()
        {
            // The base implementation disposes synchronously, which would block on the read lock.
            await CloseAsync().ConfigureAwait(false);
        }

        private bool BeginClose()
        {
            if (Interlocked.Exchange(ref _closing, 1) == 1) return false;
            // Cancelling first ends a read that is waiting on the server, so the lock is not held for long.
            _cancellation.Cancel();
            return true;
        }

        /// <summary>
        /// Shuts the reader down. Must be called while holding the read lock, which it releases.
        /// </summary>
        private void EndClose()
        {
            try
            {
                // Disposing before the end aborts the HTTP response, so the server stops sending the rest.
                Finish();
                _wait.Dispose();
                _cancellation.Dispose();
                base.Close();
            }
            finally
            {
                _readLock.Release();
            }
        }

        protected override DbDataReader GetDbDataReader(int ordinal)
        {
            throw new NotImplementedException("GetDbDataReader is not implemented because it of query result being streamed");
        }
    }
}
