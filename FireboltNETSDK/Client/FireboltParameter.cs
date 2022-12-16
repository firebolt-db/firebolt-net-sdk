using FireboltDotNetSdk.Exception;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.RegularExpressions;

namespace FireboltDotNetSdk.Client
{
    /// <summary>
    /// Represents a parameter to a <see cref="FireboltCommand"/>. This class cannot be inherited.
    /// </summary>
    public sealed class FireboltParameter : DbParameter
    {
        private static readonly Regex ParameterNameRegex = new("^[a-zA-Z_][0-9a-zA-Z_]*$");
        private string _parameterName;
        private object? _value;
        private int _size;
        private TimeZoneInfo? _timeZone;
        private bool? _forcedNullable;
        private FireboltDbType _forcedType;
        private byte? _forcedScale;
        private byte? _forcedPrecision;
        private int? _forcedArrayRank;
        private string? _sourceColumn;

        internal string Id { get; private set; }

        /// <summary>
        /// Gets the collection to which this parameter is attached.
        /// </summary>
        public FireboltParameterCollection? Collection { get; internal set; }

        /// <summary>
        /// Gets or sets the <see cref="FireboltDbType"/> of the parameter.
        /// </summary>
        /// <returns>One of the <see cref="FireboltDbType"/> values. The default value is defined based on the type of the parameter's value.</returns>
        public FireboltDbType FireboltDbType
        {
            get
            {
                return (FireboltDbType)_forcedType;
            }

            set => _forcedType = value;
        }

        /// <summary>
        /// Gets or sets the <see cref="DbType"/> of the parameter.
        /// </summary>
        /// <returns>
        /// One of the <see cref="DbType"/> values. The default value is defined based on the type of the parameter's value.
        /// For Firebolt-specific types returns <see cref="DbType.Object"/>.
        /// </returns>        
        public override DbType DbType
        {
            get
            {
                var chType = FireboltDbType;
                return (DbType)chType;
            }
            set => FireboltDbType = (FireboltDbType)value;
        }

        /// <summary>
        /// Gets the direction of the parameter. Always returns <see cref="ParameterDirection.Input"/>.
        /// </summary>
        /// <returns>Always returns <see cref="ParameterDirection.Input"/>.</returns>
        /// <exception cref="NotSupportedException">Throws <see cref="NotSupportedException"/> on attempt to set the value different from <see cref="ParameterDirection.Input"/>.</exception>
        public override ParameterDirection Direction
        {
            get => ParameterDirection.Input;
            set
            {
                if (value != ParameterDirection.Input)
                    throw new NotSupportedException("Only input parameters are supported.");
            }
        }

        /// <summary>
        /// Gets or sets the name of the parameter.
        /// </summary>
        [AllowNull]
        public sealed override string ParameterName
        {
            get => _parameterName;
            set
            {
                var id = GetId(value);
                Debug.Assert(value != null);

                if (StringComparer.Ordinal.Equals(id, Id))
                {
                    _parameterName = value;
                    return;
                }

                if (Collection == null)
                {
                    Id = id;
                    _parameterName = value;
                    return;
                }

                var oldId = Id;
                var oldParameterName = _parameterName;
                Id = id;
                _parameterName = value;

                try
                {
                    Collection.OnParameterIdChanged(oldId, this);
                }
                catch
                {
                    Id = oldId;
                    _parameterName = oldParameterName;
                    throw;
                }
            }
        }

        /// <inheritdoc/>
        /// <remarks>This property is ignored by FireboltClient.</remarks>
        [AllowNull]
        public override string SourceColumn
        {
            get => _sourceColumn ?? string.Empty;
            set => _sourceColumn = value;
        }

        /// <inheritdoc/>
        public override object? Value
        {
            get => _value;
            set
            {
                _value = value;
            }
        }


        /// <summary>
        /// Gets or sets the encoding that will be used when writing a string value to the database.
        /// </summary>
        public Encoding? StringEncoding { get; set; }

        /// <summary>
        /// This property allows to specify the timezone for datetime types (<see cref="FireboltClient.FireboltDbType.DateTime"/>,
        /// <see cref="FireboltClient.FireboltDbType.DateTimeOffset"/>, <see cref="FireboltClient.FireboltDbType.DateTime2"/>
        /// and <see cref="FireboltClient.FireboltDbType.DateTime64"/>).
        /// </summary>
        public TimeZoneInfo? TimeZone
        {
            get => _timeZone;
            set
            {
                _timeZone = value;
            }
        }

        /// <summary>
        /// Gets or sets the value indicating whether the type is an array.
        /// </summary>
        /// <returns><see langword="true"/> if the value is an array; otherwise <see langword="false"/>. The default value is defined based on the <see cref="ArrayRank"/>.</returns>
        public bool IsArray
        {
            get => ArrayRank > 0;
            set
            {
                if (value)
                {
                    if (_forcedArrayRank == null || ArrayRank == 0)
                        ArrayRank = 1;
                }
                else
                {
                    if (_forcedArrayRank == null || ArrayRank > 0)
                        ArrayRank = 0;
                }
            }
        }

        /// <summary>
        /// Gets or sets the rank (a number of dimensions) of an array.
        /// </summary>
        /// <returns>The rank of an array. 0 if the type is not an array. The default value is defined based on the type of the parameter's value.</returns>
        public int ArrayRank
        {
            get
            {
                if (_forcedArrayRank != null) return _forcedArrayRank.Value;
                return 0;
            }
            set
            {
                if (value < 0)
                    throw new ArgumentException("The rank of an array must be a non-negative number.", nameof(value));

                _forcedArrayRank = value;
            }
        }

        public override bool IsNullable { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public override int Size { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public override bool SourceColumnNullMapping { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        /// <summary>
        /// Initializes a new instance of <see cref="FireboltParameter"/> with the default name.
        /// </summary>
        public FireboltParameter()
            : this("parameter")
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="FireboltParameter"/> with the specified name.
        /// </summary>
        /// <param name="parameterName">The name of the parameter.</param>
        public FireboltParameter(string parameterName)
        {
            if (parameterName == null)
                throw new ArgumentNullException(nameof(parameterName));

            Id = GetId(parameterName);
            _parameterName = parameterName;
        }

        private static string GetId(string? parameterName)
        {
            if (!ValidateParameterName(parameterName, out var id))
                throw new ArgumentException("The name of the parameter must be a valid Firebolt identifier.", nameof(parameterName));

            return id;
        }

        private static bool ValidateParameterName(string? parameterName, [MaybeNullWhen(false)] out string id)
        {
            if (string.IsNullOrWhiteSpace(parameterName))
            {
                id = null;
                return false;
            }

            id = TrimParameterName(parameterName);
            return ParameterNameRegex.IsMatch(id);
        }

        internal static string TrimParameterName(string parameterName)
        {
            if (parameterName.Length > 0)
            {
                if (parameterName[0] == '{' && parameterName[^1] == '}')
                    return parameterName[1..^1];

                // MSSQL-style parameter name
                if (parameterName[0] == '@')
                    return parameterName[1..];
            }

            return parameterName;
        }

        public override void ResetDbType()
        {
            throw new NotImplementedException();
        }

        private class ParameterColumnTypeDescriptorAdapter
        {
            private readonly FireboltParameter _parameter;

            public string ColumnName => _parameter.Id;

            public FireboltDbType? FireboltDbType => _parameter._forcedType;

            public Type ValueType => _parameter?.Value?.GetType() ?? typeof(DBNull);

            public bool? IsNullable => _parameter._forcedNullable;

            public int Size => _parameter._size;

            public byte? Precision => _parameter._forcedPrecision;

            public byte? Scale => _parameter._forcedScale;

            public TimeZoneInfo? TimeZone => _parameter.TimeZone;

            public int? ArrayRank => _parameter._forcedArrayRank;

            public ParameterColumnTypeDescriptorAdapter(FireboltParameter parameter)
            {
                _parameter = parameter;
            }
        }
    }
}
