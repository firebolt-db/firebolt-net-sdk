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

        private readonly Stream _stream;
        private readonly byte[] _buffer = new byte[64 * 1024];
        private int _bufferStart;
        private int _bufferEnd;
        private readonly MemoryStream _partialLine = new();
        private readonly int _readTimeoutMillis;
        private readonly CancellationToken _cancellationToken;
        // Linked to the caller's token; the per-read timeout and Close cancel it too.
        private readonly CancellationTokenSource _cancellation;
        // Held for every read, so Close never disposes the stream underneath one.
        private readonly SemaphoreSlim _readLock = new(1, 1);
        private List<Meta> _metas = new();
        private readonly Queue<List<object?>> _currentRowQueue = new();
        private List<object?>? _currentRow;
        private bool _endOfStream;
        private volatile bool _closing;

        public FireboltStreamingDataReader(StreamReader streamReader) : this(streamReader.BaseStream, 0, CancellationToken.None)
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

        private FireboltStreamingDataReader(Stream stream, int readTimeoutMillis, CancellationToken cancellationToken) :
            base(null, new QueryResult())
        {
            _stream = stream;
            _readTimeoutMillis = readTimeoutMillis;
            _cancellationToken = cancellationToken;
            _cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        }

        /// <summary>
        /// Creates a reader that has already read the schema and the first rows, so <see cref="HasRows"/> is known.
        /// </summary>
        /// <param name="readTimeoutMillis">Limit for each wait on the server for more data, 0 for none.</param>
        /// <param name="cancellationToken">Cancels the stream at any point while it is being read.</param>
        internal static async Task<FireboltStreamingDataReader> CreateAsync(StreamReader streamReader, int readTimeoutMillis,
            CancellationToken cancellationToken)
        {
            var reader = new FireboltStreamingDataReader(streamReader.BaseStream, readTimeoutMillis, cancellationToken);
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

                var start = JsonConvert.DeserializeObject<StreamingJsonStart>(line);
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
                if (_endOfStream) return false;

                var line = await NextLineAsync(cancellationToken).ConfigureAwait(false);
                if (line == null) throw Fail("Stream ended before the server signalled the end of the result");
                if (string.IsNullOrWhiteSpace(line)) throw Fail("Failed to read line from stream");

                var json = JsonConvert.DeserializeObject<StreamingJsonData>(line);
                switch (json?.MessageType)
                {
                    case Data:
                        json.Data.ForEach(_currentRowQueue.Enqueue);
                        return true;
                    case FinishSuccessfully:
                        Finish();
                        return false;
                    default:
                        throw ErrorFromLine(line);
                }
            }
            finally
            {
                _readLock.Release();
            }
        }

        private System.Exception ErrorFromLine(string line)
        {
            Finish();
            StreamingJsonFinishError? json;
            try
            {
                json = JsonConvert.DeserializeObject<StreamingJsonFinishError>(line);
            }
            catch (System.Exception e)
            {
                return new FireboltException("Failed to parse JSON from stream on line: " + line, e);
            }
            // A plain JSON error body (no message_type) can still arrive if the server failed before streaming.
            if (json?.MessageType == FinishWithErrors || (json?.MessageType == null && json?.Errors != null))
                return new FireboltStructuredException(json.Errors);
            return new FireboltException("Failed to parse JSON from stream. Unexpected messageType: " + json?.MessageType + " in line: " + line);
        }

        /// <summary>
        /// Reads the next line, bounded by the read timeout. Must be called while holding the read lock.
        /// </summary>
        private async Task<string?> NextLineAsync(CancellationToken cancellationToken)
        {
            using var callCancellation = cancellationToken.CanBeCanceled
                ? CancellationTokenSource.CreateLinkedTokenSource(_cancellation.Token, cancellationToken)
                : null;
            if (_readTimeoutMillis > 0) _cancellation.CancelAfter(_readTimeoutMillis);
            try
            {
                return await ReadLineAsync(callCancellation?.Token ?? _cancellation.Token).ConfigureAwait(false);
            }
            catch (System.Exception e)
            {
                throw ReadFailure(e, cancellationToken);
            }
            finally
            {
                if (_readTimeoutMillis > 0) _cancellation.CancelAfter(Timeout.Infinite);
            }
        }

        private async Task<string?> ReadLineAsync(CancellationToken cancellationToken)
        {
            while (true)
            {
                var newline = Array.IndexOf(_buffer, (byte)'\n', _bufferStart, _bufferEnd - _bufferStart);
                if (newline >= 0)
                {
                    var line = TakeLine(newline - _bufferStart);
                    _bufferStart = newline + 1;
                    return line;
                }

                await _partialLine.WriteAsync(_buffer.AsMemory(_bufferStart, _bufferEnd - _bufferStart), cancellationToken).ConfigureAwait(false);
                _bufferStart = _bufferEnd = 0;
                // The token reaches the HTTP stack, which aborts the connection itself when it is cancelled.
                var read = await _stream.ReadAsync(_buffer.AsMemory(), cancellationToken).ConfigureAwait(false);
                if (read == 0) return _partialLine.Length == 0 ? null : TakeLine(0);
                _bufferEnd = read;
            }
        }

        private string TakeLine(int length)
        {
            string line;
            if (_partialLine.Length == 0)
            {
                line = Encoding.UTF8.GetString(_buffer, _bufferStart, length);
            }
            else
            {
                // UTF-8 never uses the newline byte inside a multi-byte character, so splitting on it is safe.
                _partialLine.Write(_buffer, _bufferStart, length);
                line = Encoding.UTF8.GetString(_partialLine.GetBuffer(), 0, (int)_partialLine.Length);
                _partialLine.SetLength(0);
            }
            return line.EndsWith('\r') ? line[..^1] : line;
        }

        private System.Exception ReadFailure(System.Exception cause, CancellationToken cancellationToken)
        {
            Finish();
            if (_closing)
                return new OperationCanceledException("The reader was closed while reading.", cause);
            if (cancellationToken.IsCancellationRequested)
                return new OperationCanceledException("The streamed query was cancelled.", cause, cancellationToken);
            if (_cancellationToken.IsCancellationRequested)
                return new OperationCanceledException("The streamed query was cancelled.", cause, _cancellationToken);
            if (_cancellation.IsCancellationRequested)
                return new FireboltTimeoutException(_readTimeoutMillis);
            return new FireboltException("Failed to read the query result from the server.", cause);
        }

        private FireboltException Fail(string message, System.Exception? cause = null)
        {
            Finish();
            return cause == null ? new FireboltException(message) : new FireboltException(message, cause);
        }

        private void Finish()
        {
            _endOfStream = true;
            // Ending the response right away returns the connection even if the caller never disposes the reader.
            _stream.Dispose();
        }

        /// <inheritdoc/>
        public override void Close()
        {
            if (_closing) return;
            _closing = true;
            // Cancelling first ends a read that is waiting on the server, so the lock is not held for long.
            _cancellation.Cancel();
            _readLock.Wait(CancellationToken.None);
            try
            {
                _currentRowQueue.Clear();
                // Disposing before the end aborts the HTTP response, so the server stops sending the rest.
                Finish();
                _cancellation.Dispose();
                _partialLine.Dispose();
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
