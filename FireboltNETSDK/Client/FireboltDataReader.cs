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
            { "real", typeof(float) },
            { "double", typeof(double) },
            { "double precision", typeof(double) },
            { "decimal", typeof(decimal) },
            { "numeric", typeof(decimal) },

            { "string", typeof(string) },
            { "text", typeof(string) },

            { "date", typeof(DateTime) },
            { "datetime", typeof(DateTime) },
            { "timestamp", typeof(DateTime) },
            { "timestamptz", typeof(DateTime) },
            { "DateTime64", typeof(DateTime) },

            { "bytea", typeof(byte[]) },
            { "array", typeof(Array) },
        };

        public FireboltDataReader(string? fullTableName, QueryResult queryResult, int depth = 0)
        {
            _fullTableName = fullTableName;
            _queryResult = queryResult;
            _depth = depth;
        }

        /// <inheritdoc/>
        public override object this[string name] { get => GetValue(GetOrdinal(name)); }

        /// <inheritdoc/>
        public override object this[int ordinal] { get => GetValue(ordinal); }

        /// <inheritdoc/>
        public override bool IsClosed { get => _closed; }

        /// <inheritdoc/>
        public override bool HasRows { get => _queryResult.Data.Count > 0; }

        /// <inheritdoc/>
        public override int FieldCount { get => _queryResult.Meta.Count; }

        /// <inheritdoc/>
        public override int Depth { get => _depth; }

        /// <inheritdoc/>
        public override int RecordsAffected { get => 0; }

        /// <inheritdoc/>
        public override int VisibleFieldCount { get => FieldCount; }

        /// <inheritdoc/>
        public override void Close()
        {
            _closed = true;
        }

        /// <inheritdoc/>
        public override Task CloseAsync()
        {
            Close();
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
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
                case string s: return TypesConverter.ParseBoolean(s);
                default: throw new InvalidCastException($"Cannot cast ({value.GetType()}){value} to boolean");
            }
        }

        /// <inheritdoc/>
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

        /// <inheritdoc/>
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        {
            object value = GetValue(ordinal);
            byte[]? bytes;
            switch (value)
            {
                case string s: bytes = Encoding.UTF8.GetBytes(s); break;
                case byte[] b: bytes = b; break;
                default: throw new InvalidCastException($"Cannot retrive byte array from ({value.GetType()}){value}");
            }
            return GetBuffer(bytes, dataOffset, buffer, bufferOffset, length);
        }

        /// <inheritdoc/>
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

        /// <inheritdoc/>
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        {
            return GetBuffer(GetString(ordinal).ToCharArray(), dataOffset, buffer, bufferOffset, length);
        }

        private long GetBuffer<T>(T[] source, long dataOffset, T[]? buffer, int bufferOffset, int length)
        {
            if (buffer == null)
            {
                return 0;
            }
            int limit = Math.Min(source.Length - (int)dataOffset, buffer.Length);
            Array.Copy(source, dataOffset, buffer, bufferOffset, limit);
            return limit;
        }

        /// <inheritdoc/>
        public override string GetDataTypeName(int ordinal)
        {
            return _queryResult.Meta[ordinal].Type;
        }

        /// <inheritdoc/>
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

        /// <inheritdoc/>
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

        /// <inheritdoc/>
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

        /// <inheritdoc/>
        //[EditorBrowsable(EditorBrowsableState.Never)]
        public override IEnumerator GetEnumerator()
        {
            return new DbEnumerator(this);
        }

        /// <inheritdoc/>
        public override Type GetFieldType(int ordinal)
        {
            return GetTypeByName(GetDataTypeName(ordinal)) ?? throw new ArgumentNullException($"Cannot get type of column #{ordinal}");
        }

        private Type GetTypeByName(string typeName)
        {
            typeName = Remove(typeName, @"\s+(?:not\s+)?null");
            if (IsArrayType(typeName))
            {
                return GetArrayTypeByName(typeName, 0);
            }
            typeName = Remove(typeName, @"\(.*");
            return typesMap[typeName] ?? typeof(object);
        }

        private string Remove(string str, string regex)
        {
            return Regex.Replace(str, regex, "", RegexOptions.IgnoreCase);
        }

        private bool IsArrayType(string typeName)
        {
            return typeName.ToLower().StartsWith("array");
        }

        private Type GetArrayTypeByName(string typeName, int dim)
        {
            if (!IsArrayType(typeName))
            {
                return CreateArrayType(typeName, dim);
            }
            int left = typeName.IndexOf("(");
            int right = typeName.LastIndexOf(")");
            if (Math.Sign(left) != Math.Sign(right))
            {
                throw new InvalidCastException($"Cannot cast {typeName} to array");
            }
            if (left < 0 && right < 0)
            {
                return CreateArrayType(typeName, dim);
            }
            return GetArrayTypeByName(typeName.Substring(left + 1, right - left - 1), dim + 1);
        }

        private Type CreateArrayType(string elementTypeName, int dim)
        {
            Type type = GetTypeByName(elementTypeName);
            for (int i = 0; i < dim; i++)
            {
                type = type.MakeArrayType();
            }
            return type;
        }

        /// <inheritdoc/>
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

        /// <summary>
        /// Gets the value of the specified column as a globally unique identifier (GUID).
        /// Due to Firebolt does not support GUID as a base type this function works only if the value is string that can be parsed as GUID.
        /// Otherwise <exception cref="InvalidCastException">InvalidCastException</exception> is thrown. 
        /// </summary>
        /// <exception cref="FormatException">If format is invalid</exception>
        /// <exception cref="OverflowException">If format is invalid</exception>
        public override Guid GetGuid(int ordinal)
        {
            object value = GetValue(ordinal);
            switch (value)
            {
                case string s: return new Guid(s);
                default: throw new InvalidCastException($"Cannot cast ({value.GetType()}){value} to GUID");
            }
        }

        /// <inheritdoc/>
        public override short GetInt16(int ordinal)
        {
            object value = ThrowIfInfinityOrNaN(GetValue(ordinal), typeof(short));
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

        /// <inheritdoc/>
        public override int GetInt32(int ordinal)
        {
            object value = ThrowIfInfinityOrNaN(GetValue(ordinal), typeof(int));
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

        /// <inheritdoc/>
        public override long GetInt64(int ordinal)
        {
            object value = ThrowIfInfinityOrNaN(GetValue(ordinal), typeof(long));
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

        private static object ThrowIfInfinityOrNaN(object value, Type type)
        {
            if (TypesConverter.isInfinity(value) || TypesConverter.isNaN(value))
            {
                throw new InvalidCastException($"Cannot convert {value} to {type.Name}");
            }
            return value;
        }

        /// <inheritdoc/>
        public override string GetName(int ordinal)
        {
            return _queryResult.Meta[ordinal].Name;
        }

        /// <inheritdoc/>
        public override int GetOrdinal(string name)
        {
            return _queryResult?.Meta.FindIndex(m => m.Name == name) ?? throw new IndexOutOfRangeException($"Cannot find index for column {name}");
        }

        /// <inheritdoc/>
        public override DataTable? GetSchemaTable()
        {
            return _fullTableName == null ? null : new DataTable(_fullTableName); // TODO split full table name that can contain schema_name.table_name (dot notation)
        }

        /// <inheritdoc/>
        public override Task<DataTable?> GetSchemaTableAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromResult(GetSchemaTable());
        }

        public override string GetString(int ordinal)
        /// <inheritdoc/>
        {
            return GetValue(ordinal).ToString() ?? throw new InvalidOperationException($"String representation of column ${ordinal} is null");
        }

        /// <inheritdoc/>
        public override object GetValue(int ordinal)
        {
            return GetValueSafely(ordinal) ?? throw new InvalidOperationException($"Column ${ordinal} is null");
        }

        /// <inheritdoc/>
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
                values[i] = GetValue(i);
            }
            return n;
        }

        /// <inheritdoc/>
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

        /// <summary>
        /// Throws exception <exception cref="FireboltException">FireboltException</exception> because batch of read operations is not supported.
        /// </summary>
        /// <exception cref="FireboltException"></exception>
        public override bool NextResult()
        {
            return false;
        }

        /// <inheritdoc/>
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
