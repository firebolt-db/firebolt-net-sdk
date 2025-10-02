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
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using FireboltDoNetSdk.Utils;
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;
using FireboltNETSDK.Exception;
using Newtonsoft.Json;

namespace FireboltDotNetSdk.Client
{
    /// <summary>
    /// Represents a data reader to stream data form a FireBolt database. This class cannot be inherited.
    /// </summary>
    public sealed class FireboltStreamingDataReader : FireboltDataReader
    {
        private readonly StreamReader _streamReader;
        private List<Meta> _metas = new();
        private readonly Queue<List<object?>> _currentRowQueue = new();
        private List<object?>? _currentRow;
        private bool _endOfStream = false;
        private const string Start = "START";
        private const string Data = "DATA";
        private const string FinishWithErrors = "FINISH_WITH_ERRORS";
        private const string FinishSuccessfully = "FINISH_SUCCESSFULLY";

        public FireboltStreamingDataReader(StreamReader streamReader) :
            base(null, new QueryResult())
        {
            _streamReader = streamReader;
            InitializeMetadata();
            FetchRow();
        }

        /// <inheritdoc/>
        public override bool HasRows => _currentRowIndex > 0 || _currentRowQueue.Count > 0;

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
            if (IsStreamClosed()) return false;
            if (_currentRowQueue.Count == 0 && !FetchRow())
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

        private bool FetchRow()
        {
            if (IsStreamClosed()) return false;

            var line = _streamReader.ReadLine();
            if (string.IsNullOrWhiteSpace(line)) throw new FireboltException("Failed to read line from stream");

            var json = JsonConvert.DeserializeObject<StreamingJsonData>(line);
            var messageType = json?.MessageType;

            switch (messageType)
            {
                case Data:
                    json!.Data.ForEach(_currentRowQueue.Enqueue);
                    return true;

                case FinishSuccessfully:
                    _endOfStream = true;
                    break;

                default:
                    HandleError(line);
                    break;
            }

            return false;
        }

        private void InitializeMetadata()
        {
            var line = _streamReader.ReadLine();
            if (string.IsNullOrWhiteSpace(line))
            {
                _endOfStream = true;
                throw new FireboltException("Failed to read line from stream");
            }

            var json = JsonConvert.DeserializeObject<StreamingJsonStart>(line);

            if (json?.MessageType != Start) HandleError(line);
            _metas = json?.Meta ?? new List<Meta>();
        }

        private void HandleError(string line)
        {
            _endOfStream = true;
            StreamingJsonFinishError? json;
            try
            {
                json = JsonConvert.DeserializeObject<StreamingJsonFinishError>(line);
            }
            catch (System.Exception e)
            {
                throw new FireboltException("Failed to parse JSON from stream on line: " + line, e);
            }
            if (json?.MessageType == FinishWithErrors)
                throw new FireboltStructuredException(json.Errors);
            throw new FireboltException("Failed to parse JSON from stream. Unexpected messageType: " + json?.MessageType + " in line: " + line);
        }

        private bool IsStreamClosed()
        {
            if (!_streamReader.EndOfStream || _endOfStream) return false;
            _endOfStream = true;
            return true;

        }

        protected override DbDataReader GetDbDataReader(int ordinal)
        {
            throw new NotImplementedException("GetDbDataReader is not implemented because it of query result being streamed");
        }
    }
}
