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
using System.Text.RegularExpressions;
using FireboltDoNetSdk.Utils;
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;

namespace FireboltDotNetSdk.Client
{
    /// <summary>
    /// Represents an data reader to get data form a FireBolt database. This class cannot be inherited.
    /// </summary>
    public sealed class FireboltDataReader : DbDataReader
    {
        private bool _closed = false;
        private string? _fullTableName;
        private QueryResult _queryResult;
        private int _depth;
        private int _currentRowIndex = -1;
        private static IDictionary<string, Type> typesMap = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase)
        {
            { "boolean", typeof(bool) },

            { "byte", typeof(byte) },
            { "short", typeof(short) },
            { "int16", typeof(short) },
            { "int", typeof(int) },
            { "integer", typeof(int) },
            { "int32", typeof(int) },
            { "bigint", typeof(long) },
            { "long", typeof(long) },
            { "int64", typeof(long) },

            { "float", typeof(float) },
            { "double", typeof(double) },
            { "double precision", typeof(double) },
            { "decimal", typeof(decimal) },

            { "string", typeof(string) },
            { "text", typeof(string) },

            { "date", typeof(DateTime) },
            { "datetime", typeof(DateTime) },
            { "timestamp", typeof(DateTime) },
            { "timestamptz", typeof(DateTime) },
            { "DateTime64", typeof(DateTime) },
        };

        public FireboltDataReader(string? fullTableName, QueryResult queryResult, int depth = 0)
        {
            _fullTableName = fullTableName;
            _queryResult = queryResult;
            _depth = depth;
        }

        public override object this[string name] { get => GetValue(GetOrdinal(name)); }

        public override object this[int ordinal] { get => GetValue(ordinal); }

        public override bool IsClosed { get => _closed; }

        public override bool HasRows { get => _queryResult.Data.Count > 0; }

        public override int FieldCount { get => _queryResult.Meta.Count; }

        public override int Depth { get => _depth; }

        public override int RecordsAffected { get => 0; }

        public override int VisibleFieldCount { get => FieldCount; }

        public override void Close()
        {
            _closed = true;
        }

        public override Task CloseAsync()
        {
            Close();
            return Task.CompletedTask;
        }

        public override bool GetBoolean(int ordinal)
        {
            object value = GetValue(ordinal);
            switch (value)
            {
                case bool b: return b;
                case float f: return (float)f != 0;
                case double d: return (double)d != 0;
                case decimal d: return d != 0;
                case byte b: return b != 0;
                case short s: return s != 0;
                case int i: return i != 0;
                case long l: return l != 0;
                case string s: return bool.Parse(s);
                default: throw new InvalidCastException($"Cannot cast ({value.GetType()}){value} to boolean");
            }
        }

        public override byte GetByte(int ordinal)
        {
            object value = GetValue(ordinal);
            switch (value)
            {
                case float f: return (byte)f;
                case double d: return (byte)d;
                case decimal d: return (byte)d;
                case byte b: return b;
                case short s: return (byte)s;
                case int i: return (byte)i;
                case long l: return (byte)l;
                case string s: return byte.Parse(s);
                default: throw new InvalidCastException($"Cannot cast ({value.GetType()}){value} to byte");
            }
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        {
            return GetBuffer<byte>(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        public override char GetChar(int ordinal)
        {
            object value = GetValue(ordinal);
            switch (value)
            {
                case float f: return (char)f;
                case double d: return (char)d;
                case decimal d: return (char)d;
                case byte b: return (char)b;
                case short s: return (char)s;
                case int i: return (char)i;
                case long l: return (char)l;
                case string s: return s[0];
                default: throw new InvalidCastException($"Cannot cast ({value.GetType()}){value} to char");
            }
        }

        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        {
            return GetBuffer<char>(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        private long GetBuffer<T>(int ordinal, long dataOffset, T[]? buffer, int bufferOffset, int length)
        {
            if (buffer == null)
            {
                return 0;
            }
            string str = GetString(ordinal);
            System.Buffer.BlockCopy(str.ToCharArray(), (int)dataOffset, buffer, bufferOffset, length);
            return Math.Min(length, str.Length - (int)dataOffset);
        }

        public override string GetDataTypeName(int ordinal)
        {
            return _queryResult.Meta[ordinal].Type;
        }

        public override DateTime GetDateTime(int ordinal)
        {
            object value = GetValue(ordinal);
            switch (value)
            {
                case DateTime dt: return dt;
                case DateOnly d: return d.ToDateTime(new TimeOnly(0, 0, 0));
                case string s: return TypesConverter.ParseDateTime(s);
                default: throw new InvalidCastException($"Cannot cast ({value.GetType()}){value} to DateTime");
            }
        }

        public override decimal GetDecimal(int ordinal)
        {
            object value = GetValue(ordinal);
            switch (value)
            {
                case float f: return (decimal)f;
                case double d: return (decimal)d;
                case decimal d: return d;
                case byte b: return b;
                case short s: return s;
                case int i: return i;
                case long l: return l;
                case string s: return decimal.Parse(s);
                default: throw new InvalidCastException($"Cannot cast ({value.GetType()}){value} to double");
            }
        }

        public override double GetDouble(int ordinal)
        {
            object value = GetValue(ordinal);
            switch (value)
            {
                case float f: return f;
                case double d: return d;
                case decimal d: return (double)d;
                case byte b: return b;
                case short s: return s;
                case int i: return i;
                case long l: return l;
                case string s: return double.Parse(s);
                default: throw new InvalidCastException($"Cannot cast ({value.GetType()}){value} to double");
            }
        }

        //[EditorBrowsable(EditorBrowsableState.Never)]
        public override IEnumerator GetEnumerator()
        {
            return new DbEnumerator(this);
        }

        public override Type GetFieldType(int ordinal)
        {
            return GetTypeByName(GetDataTypeName(ordinal)) ?? throw new ArgumentNullException($"Cannot get type of column #{ordinal}");
        }

        private Type? GetTypeByName(string typeName)
        {
            typeName = Regex.Replace(typeName, @"\s+null", "", RegexOptions.IgnoreCase);
            return typesMap[typeName] ?? typeof(object);
        }

        public override float GetFloat(int ordinal)
        {
            object value = GetValue(ordinal);
            switch (value)
            {
                case float f: return f;
                case double d: return (float)d;
                case decimal d: return (float)d;
                case byte b: return b;
                case short s: return s;
                case int i: return i;
                case long l: return l;
                case string s: return float.Parse(s);
                default: throw new InvalidCastException($"Cannot cast ({value.GetType()}){value} to float");
            }
        }

        public override Guid GetGuid(int ordinal)
        {
            object value = GetValue(ordinal);
            switch (value)
            {
                case string s: return new Guid(s);
                default: throw new InvalidCastException($"Cannot cast ({value.GetType()}){value} to GUID");
            }
        }

        public override short GetInt16(int ordinal)
        {
            object value = ThrowIfInfinity(GetValue(ordinal), typeof(short));
            switch (value)
            {
                case float f: return (short)f;
                case double d: return (short)d;
                case decimal d: return (short)d;
                case byte b: return b;
                case short s: return s;
                case int i: return (short)i;
                case long l: return (short)l;
                case string s: return short.Parse(s);
                default: throw new InvalidCastException($"Cannot cast ({value.GetType()}){value} to short");
            }
        }

        public override int GetInt32(int ordinal)
        {
            object value = ThrowIfInfinity(GetValue(ordinal), typeof(int));
            switch (value)
            {
                case float f: return (int)f;
                case double d: return (int)d;
                case decimal d: return (int)d;
                case byte b: return b;
                case short s: return s;
                case int i: return i;
                case long l: return (int)l;
                case string s: return int.Parse(s);
                default: throw new InvalidCastException($"Cannot cast ({value.GetType()}){value} to int");
            }
        }
        public override long GetInt64(int ordinal)
        {
            object value = ThrowIfInfinity(GetValue(ordinal), typeof(long));
            switch (value)
            {
                case float f: return (long)f;
                case double d: return (long)d;
                case decimal d: return (long)d;
                case byte b: return b;
                case short s: return s;
                case int i: return i;
                case long l: return l;
                case string s: return long.Parse(s);
                default: throw new InvalidCastException($"Cannot cast ({value.GetType()}){value} to long");
            }
        }

        private static object ThrowIfInfinity(object value, Type type)
        {
            if (TypesConverter.isInfinity(value))
            {
                throw new InvalidCastException($"Cannot convert {value} to {type.Name}");
            }
            return value;
        }

        public override string GetName(int ordinal)
        {
            return _queryResult.Meta[ordinal].Name;
        }

        public override int GetOrdinal(string name)
        {
            return _queryResult?.Meta.FindIndex(m => m.Name == name) ?? throw new IndexOutOfRangeException($"Cannot find index for column {name}");
        }

        public override DataTable? GetSchemaTable()
        {
            return _fullTableName == null ? null : new DataTable(_fullTableName); // TODO split full table name that can contain schema_name.table_name (dot notation)
        }

        public override Task<DataTable?> GetSchemaTableAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromResult(GetSchemaTable());
        }

        public override Stream GetStream(int ordinal)
        {
            string str = GetString(ordinal);
            byte[] bytes = new byte[str.Length * sizeof(char)];
            System.Buffer.BlockCopy(str.ToCharArray(), 0, bytes, 0, bytes.Length);
            return new MemoryStream(bytes);
        }

        public override string GetString(int ordinal)
        {
            return GetValue(ordinal).ToString() ?? throw new InvalidOperationException($"String representation of column ${ordinal} is null");
        }

        public override TextReader GetTextReader(int ordinal)
        {
            return new StringReader(GetString(ordinal));
        }

        public override object GetValue(int ordinal)
        {
            return GetValueSafely(ordinal) ?? throw new InvalidOperationException($"Column ${ordinal} is null");
        }

        public override int GetValues(object[] values)
        {
            List<object?> row = _queryResult.Data[_currentRowIndex] ?? new List<object?>();
            if (row == null)
            {
                return 0;
            }
            int n = Math.Min(row.Count, values.Length);
            for (int i = 0; i < n; i++)
            {
                values[i] = row[i] ?? throw new InvalidOperationException($"Column ${i} is null");
            }
            return n;
        }

        public override bool IsDBNull(int ordinal)
        {
            return GetValueSafely(ordinal) == DBNull.Value;
        }

        private object? GetValueSafely(int ordinal)
        {
            List<object?>? row = _queryResult.Data[_currentRowIndex];
            if (ordinal < 0 || ordinal > row?.Count - 1)
            {
                throw new IndexOutOfRangeException($"Column ${ordinal} does not exist");
            }
            if (row == null)
            {
                return null;
            }
            object? value = row[ordinal];
            if (value == null)
            {
                return DBNull.Value;
            }
            var columnType = ColumnType.Of(TypesConverter.GetFullColumnTypeName(_queryResult.Meta[ordinal]));
            return TypesConverter.ConvertToCSharpVal(value.ToString(), columnType);
        }

        new public Task<bool> IsDBNullAsync(int ordinal)
        {
            return Task.FromResult(IsDBNull(ordinal));
        }

        public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
        {
            return Task.FromResult(IsDBNull(ordinal));
        }

        public override bool NextResult()
        {
            throw new FireboltException("Batch operations are not supported");
        }

        public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(NextResult());
        }

        new public Task<bool> NextResultAsync()
        {
            return NextResultAsync(CancellationToken.None);
        }

        public override bool Read()
        {
            int? max = (_queryResult?.Data?.Count ?? 0) - 1;
            if (_currentRowIndex < max)
            {
                _currentRowIndex++;
                return true;
            }
            return false;
        }

        public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            return ReadAsync();
        }

        protected override void Dispose(bool disposing)
        {
            Dispose();
        }

        protected override DbDataReader GetDbDataReader(int ordinal)
        {
            QueryResult queryResult = new QueryResult
            {
                Query = _queryResult.Query,
                Meta = new List<Meta>() { _queryResult.Meta[ordinal] },
                Statistics = _queryResult.Statistics,
                Data = _queryResult.Data.Select(HasRows => new List<object?>() { HasRows[ordinal] }).ToList()
            };
            return new FireboltDataReader(_fullTableName, queryResult, _depth + 1);
        }
    }
}
