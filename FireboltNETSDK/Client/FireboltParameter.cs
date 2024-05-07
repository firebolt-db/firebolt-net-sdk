using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.RegularExpressions;
using System.Collections;

namespace FireboltDotNetSdk.Client
{
    /// <summary>
    /// Represents a parameter to a <see cref="FireboltCommand"/>. This class cannot be inherited.
    /// </summary>
    public sealed class FireboltParameter : DbParameter
    {
        internal const string defaultParameterName = "parameter";
        private static readonly Regex ParameterNameRegex = new Regex("^[a-zA-Z_][0-9a-zA-Z_]*$");
        private static ISet<DbType> unsupportedTypes = new HashSet<DbType>()
        {
            DbType.Currency, DbType.VarNumeric, DbType.AnsiStringFixedLength, DbType.StringFixedLength, DbType.Xml, DbType.DateTime2, DbType.Single
        };

        private static IDictionary<Type, DbType?> dbTypes = new Dictionary<Type, DbType?>
        {
            { typeof(byte), DbType.Byte },
            { typeof(bool), DbType.Boolean },
            { typeof(short), DbType.Int16 },
            { typeof(int), DbType.Int32 },
            { typeof(long), DbType.Int64 },
            { typeof(sbyte), DbType.SByte },
            { typeof(ushort), DbType.UInt16 },
            { typeof(uint), DbType.UInt32 },
            { typeof(ulong), DbType.UInt64 },
            { typeof(float), DbType.Decimal },
            { typeof(double), DbType.Double },
            { typeof(decimal), DbType.Decimal },
            { typeof(Guid), DbType.Guid },
            { typeof(string), DbType.String },
            { typeof(byte[]), DbType.Binary },
            { typeof(DateOnly), DbType.Date },
            { typeof(DateTime), DbType.DateTime },
            { typeof(DateTimeOffset), DbType.DateTimeOffset },
            { typeof(TimeOnly), DbType.Time },
        };

        private static IDictionary<Type, int> dbTypeSizes = new Dictionary<Type, int>
        {
            { typeof(byte), 1 },
            { typeof(bool), 1 },
            { typeof(short), 2 },
            { typeof(int), 4 },
            { typeof(long), 8 },
            { typeof(sbyte), 1 },
            { typeof(ushort), 2 },
            { typeof(uint), 4 },
            { typeof(ulong), 8 },
            { typeof(float), 4 },
            { typeof(double), 8 },
            { typeof(decimal), 16 },
            { typeof(Guid), 16 },
            { typeof(string), int.MaxValue }, // limitation of C#. Firebolt DB does not have limitation on string  size
            { typeof(byte[]), int.MaxValue },
            { typeof(DateOnly), 4 },
            { typeof(DateTime), 16 },
            { typeof(DateTimeOffset), 10 },
            { typeof(TimeOnly), 8 },
        };

        // This constant is copied from Microsoft's System.Data.SqlTypes.SQLDecimal, but its value is changed from 17 to 15 according to the Firebolt standard
        private const uint DBL_DIG = 15; // Max decimal digits of double
        private const int MAX_COLUMN_SIZE = 100 * 1024 * 1024; //  100 MB

        private string _parameterName;
        private object? _value;
        private int? _size;
        private bool _nullable = true;
        private bool _sourceColumnNullable = true;
        private DbType? _dbType;
        private DbType? _initialDbType;
        private string? _sourceColumn;
        internal string Id { get; private set; }

        /// <summary>
        /// Gets the collection to which this parameter is attached.
        /// </summary>
        public FireboltParameterCollection? Collection { get; internal set; }

        /// <summary>
        /// Gets or sets the <see cref="DbType"/> of the parameter.
        /// </summary>
        /// <returns>
        /// One of the <see cref="DbType"/> values. The default value is defined based on the type of the parameter's value.
        /// For Firebolt-specific types returns <see cref="DbType.Object"/>.
        /// </returns>        
        public override DbType DbType
        {
            get => _dbType ?? throw new InvalidOperationException("Type of parameter is not initialized");
            set
            {
                if (unsupportedTypes.Contains(value))
                {
                    throw new NotSupportedException($"Parameter type {value} is not supported");
                }
                _dbType = value;
            }
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
                _parameterName = value ?? throw new ArgumentNullException();
                Id = GetId(value);
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
                if (_size == null)
                {
                    _size = GetSize(value);
                }
            }
        }

        /// <summary>
        /// Gets or sets the encoding that will be used when writing a string value to the database.
        /// </summary>
        public Encoding? StringEncoding { get; set; }

        /// <inheritdoc/>
        /// <exception cref="ArgumentException">When attempting to make value of not nullable type nullable.</exception>
        public override bool IsNullable
        {
            get => _nullable;
            set
            {
                if (value && !_sourceColumnNullable)
                {
                    throw new ArgumentException($"Parameter {_parameterName} cannot be null because it is mapped to not nullable column");
                }
                _nullable = value;
            }
        }

        /// <summary>
        /// Gets the number of decimal places: 15 for <see cref="DbType.Decimal"/>, 0 otherwise.
        /// </summary>
        /// <exception cref="NotImplementedException">When set is called.</exception>
        public override byte Scale
        {
            get => (byte)(_dbType == DbType.Decimal ? DBL_DIG : 0);
            set => throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public override int Size { get => _size ?? 0; set => _size = value; }

        /// <inheritdoc/>
        public override bool SourceColumnNullMapping { get => _sourceColumnNullable; set => _sourceColumnNullable = value; }

        /// <summary>
        /// Initializes a new instance of <see cref="FireboltParameter"/> with the default name.
        /// </summary>
        public FireboltParameter() : this(defaultParameterName, null)
        {
        }

        public FireboltParameter(string parameterName, object? value) : this(parameterName, GetType(value), value)
        {
        }

        public FireboltParameter(string parameterName, DbType? dbType, object? value)
        {
            Id = GetId(parameterName);
            _parameterName = parameterName;
            if (dbType != null)
            {
                DbType = dbType ?? throw new InvalidOperationException();
                _initialDbType = DbType;
            }
            else if (value != null)
            {
                DbType = GetType(value) ?? throw new InvalidOperationException();
                _initialDbType = DbType;
            }
            _value = value;
        }

        /// <inheritdoc/>
        public override void ResetDbType()
        {
            _dbType = _initialDbType;
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

        private static int GetSize(object? value)
        {
            //TODO: improve this: some types (e.g. string, decimal) can be limited on table level
            return GetData(dbTypeSizes, value, MAX_COLUMN_SIZE, MAX_COLUMN_SIZE);
        }

        private static DbType? GetType(object? value)
        {
            return GetData(dbTypes, value, null, DbType.Object);
        }

        private static T? GetData<T>(IDictionary<Type, T> data, object? value, T? nullValueData, T collectionData)
        {
            if (value == null)
            {
                return nullValueData;
            }
            Type type = value.GetType();
            if (typeof(IList).IsAssignableFrom(type)) // works for lists and arrays
            {
                return collectionData;
            }
            return data[type];
        }
    }
}
