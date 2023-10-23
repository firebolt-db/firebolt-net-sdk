/*
 * Copyright (c) 2022 FireBolt All rights reserved.
 */

using System.Collections;
using System.Data;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NodaTime.Text;

namespace FireboltDoNetSdk.Utils
{
    public enum FireboltDataType
    {
        String, Long, Int, Float, Double, Null, Decimal, Date, DateTime, TimestampNtz, TimestampTz,
        Boolean, Array, Short, ByteA
    }
    public static class TypesConverter
    {

        //Regex that matches the string Nullable(<type>), where type is the type that we need to capture.
        private const string NullableTypePattern = @"Nullable\(([^)]+)\)";
        private const string ByteAPrefix = "\\x";
        internal static IDictionary<string, double> doubleInfinity = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase)
        {
            { "inf", double.PositiveInfinity },
            { "+inf", double.PositiveInfinity },
            { "-inf", double.NegativeInfinity },
        };
        internal static IDictionary<string, float> floatInfinity = new Dictionary<string, float>(StringComparer.OrdinalIgnoreCase)
        {
            { "inf", float.PositiveInfinity },
            { "+inf", float.PositiveInfinity },
            { "-inf", float.NegativeInfinity },
        };
        internal static ISet<object> infinityValues = new HashSet<object>()
        {
            double.PositiveInfinity, double.NegativeInfinity, float.PositiveInfinity, float.NegativeInfinity
        };

        public static object? ConvertToCSharpVal(string? val, ColumnType columnType)
        {
            if (val == null)
            {
                return null;
            }
            string str = val;
            switch (columnType.Type)
            {
                case FireboltDataType.Long:
                    return Convert.ToInt64(str);
                case FireboltDataType.Int:
                    return Convert.ToInt32(str);
                case FireboltDataType.Decimal:
                    return Convert.ToDecimal(str);
                case FireboltDataType.String:
                    return str;
                case FireboltDataType.DateTime:
                case FireboltDataType.TimestampTz:
                case FireboltDataType.TimestampNtz:
                case FireboltDataType.Date:
                    return ParseDateTime(str);
                case FireboltDataType.Short:
                    return Convert.ToInt16(str);
                case FireboltDataType.Double:
                    return doubleInfinity.ContainsKey(str) ? doubleInfinity[str] : Convert.ToDouble(str, CultureInfo.InvariantCulture);
                case FireboltDataType.Float:
                    return floatInfinity.ContainsKey(str) ? floatInfinity[str] : Convert.ToSingle(str, CultureInfo.InvariantCulture);
                case FireboltDataType.Boolean:
                    return bool.Parse(str);
                case FireboltDataType.Array:
                    return ArrayHelper.TransformToSqlArray(str, columnType);
                case FireboltDataType.ByteA:
                    return Convert.FromHexString(str.Remove(0, 2));
                case FireboltDataType.Null:
                    throw new FireboltException("Not null value in null type");
                default:
                    throw new FireboltException("Invalid destination type: " + columnType.Type);
            }
        }

        public static DateTime ParseDateTime(string str)
        {
            try
            {
                return DateTime.ParseExact(str, new[]
                {
                   "yyyy-MM-dd HH:mm:ss.FFFFFF", // dateTime without timezone
                   "yyyy-MM-dd HH:mm:ss.FFFFFFz", // dateTime with timezone in format +00
                   "yyyy-MM-dd HH:mm:ss.FFFFFFzzz", // dateTime with timezone in format +00:00
                    "yyyy-MM-dd" // date only
                }, CultureInfo.InvariantCulture);
            }
            catch (FormatException)
            {
                //DateTime.ParseExact does not handle timezones with seconds, so why we try with one last format that supports tz +00:00:00 with OffsetDateTimePattern
                var pattern = OffsetDateTimePattern.CreateWithInvariantCulture("yyyy-MM-dd HH:mm:ss.FFFFFFo<+HH:mm:ss>");
                return pattern.Parse(str).Value.InFixedZone().ToDateTimeUtc().ToLocalTime();
            }
        }

        public static FireboltDataType MapColumnTypeToFireboltDataType(string columnType)
        {
            var csharpType = columnType.ToLower() switch
            {
                "string" => FireboltDataType.String,
                "long" => FireboltDataType.Long,
                "short" => FireboltDataType.Short,
                "int" => FireboltDataType.Int,
                "integer" => FireboltDataType.Int,
                "float" => FireboltDataType.Float,
                "double" => FireboltDataType.Double,
                "text" => FireboltDataType.String,
                "date_ext" => FireboltDataType.Date,
                "date" => FireboltDataType.Date,
                "pgdate" => FireboltDataType.Date,
                "timestamp" => FireboltDataType.DateTime,
                "timestamp_ext" => FireboltDataType.DateTime,
                "timestampntz" => FireboltDataType.TimestampNtz,
                "timestamptz" => FireboltDataType.TimestampTz,
                "datetime" => FireboltDataType.DateTime,
                "null" => FireboltDataType.Null,
                "nothing" => FireboltDataType.Null,
                "nullable" => FireboltDataType.Null,
                "decimal" => FireboltDataType.Decimal,
                "numeric" => FireboltDataType.Decimal,
                "boolean" => FireboltDataType.Boolean,
                "array" => FireboltDataType.Array,
                "bytea" => FireboltDataType.ByteA,
                _ => throw new FireboltException("The data type returned from the server is not supported: " + columnType),
            };
            return csharpType;
        }

        public static string GetFullColumnTypeName(Meta meta)
        {
            Match nullableMatch = Regex.Match(meta.Type, NullableTypePattern);
            var type = nullableMatch.Success ? nullableMatch.Groups[1].Value : meta.Type;
            return type;
        }

        public static IEnumerable<NewMeta> ParseJsonResponse(string? response)
        {
            if (response == null)
            {
                throw new FireboltException("JSON data is missing");
            }
            try
            {
                var prettyJson = JToken.Parse(response).ToString(Formatting.Indented);
                var data = JsonConvert.DeserializeObject<QueryResult>(prettyJson);
                if (data == null)
                {
                    throw new FireboltException("Unable to parse data to JSON");
                }
                return ProcessQueryResult(data);
            }
            catch (Exception e)
            {
                throw new FireboltException("Error while parsing response", e);
            }
        }

        private static IEnumerable<NewMeta> ProcessQueryResult(QueryResult result)
        {
            var newListData = new List<NewMeta>();
            foreach (var t in result.Data)
                for (var j = 0; j < t.Count; j++)
                {
                    var columnType = ColumnType.Of(GetFullColumnTypeName(result.Meta[j]));
                    newListData.Add(new NewMeta
                    (
                        data: new ArrayList { ConvertToCSharpVal(t[j]?.ToString(), columnType) },
                        meta: columnType.Type.ToString()
                    ));
                }
            return newListData;
        }

        public static bool isInfinity(object value)
        {
            return infinityValues.Contains(value);
        }
    }
}



public class NewMeta
{
    public NewMeta(ArrayList data, string meta)
    {
        Data = data;
        Meta = meta;
    }
    public ArrayList Data { get; set; }
    public string Meta { get; set; }
}

/// <summary>
/// Specifies the data type of a field (column) or a <see cref="FireboltParameter"/> object.
/// </summary>
public enum FireboltDbType
{
    /// <summary>
    /// The type is not supported by the client. An encoding should be defined explicitly via <see cref="FireboltParameter.StringEncoding"/>
    /// or <see cref="FireboltColumnSettings.StringEncoding"/>.
    /// </summary>
    /// <remarks>This value corresponds to <see cref="DbType.AnsiString"/>.</remarks>
    AnsiString = DbType.AnsiString,

    /// <summary>
    /// An array of bytes.
    /// </summary>
    /// <remarks>This value corresponds to <see cref="DbType.Binary"/>.</remarks>
    Binary = DbType.Binary,

    /// <summary>
    /// An 8-bit unsigned integer ranging in value from 0 to 255.
    /// </summary>
    /// <remarks>This value corresponds to <see cref="DbType.Byte"/>.</remarks>
    Byte = DbType.Byte,

    /// <summary>
    /// A simple type representing Boolean values of <see langword="true"/> or <see langword="false"/>.        
    /// </summary>
    /// <remarks>This value corresponds to <see cref="DbType.Boolean"/>.</remarks>
    Boolean = DbType.Boolean,

    /// <summary>
    /// A type representing a date value without a time.
    /// </summary>
    /// <remarks>This value corresponds to <see cref="DbType.Date"/>.</remarks>
    Date = DbType.Date,

    /// <summary>
    /// A type representing a date and time value.
    /// </summary>
    /// <remarks>This value corresponds to <see cref="DbType.DateTime"/>.</remarks>
    DateTime = DbType.DateTime,

    /// <summary>
    /// The Firebolt type Decimal(38, 9).
    /// </summary>
    /// <remarks>This value corresponds to <see cref="DbType.Decimal"/>.</remarks>
    Decimal = DbType.Decimal,

    /// <inheritdoc cref="DbType.Double"/>
    /// <remarks>This value corresponds to <see cref="DbType.Double"/>.</remarks>
    Double = DbType.Double,

    /// <inheritdoc cref="DbType.Int16"/>
    /// <remarks>This value corresponds to <see cref="DbType.Int16"/>.</remarks>
    Int16 = DbType.Int16,

    /// <inheritdoc cref="DbType.Int32"/>
    /// <remarks>This value corresponds to <see cref="DbType.Int32"/>.</remarks>
    Int32 = DbType.Int32,

    /// <inheritdoc cref="DbType.Int64"/>
    /// <remarks>This value corresponds to <see cref="DbType.Int64"/>.</remarks>
    Int64 = DbType.Int64,

    /// <summary>
    /// A general type representing a value of either an unknown type or the Firebolt type Nothing.
    /// </summary>
    /// <remarks>This value corresponds to <see cref="DbType.Object"/>.</remarks>
    Object = DbType.Object,

    /// <inheritdoc cref="DbType.SByte"/>
    /// <remarks>This value corresponds to <see cref="DbType.SByte"/>.</remarks>
    SByte = DbType.SByte,

    /// <inheritdoc cref="DbType.Single"/>
    /// <remarks>This value corresponds to <see cref="DbType.Single"/>.</remarks>
    Single = DbType.Single,

    /// <summary>
    /// A variable-length string.
    /// </summary>
    /// <remarks>This value corresponds to <see cref="DbType.String"/>.</remarks>
    String = DbType.String,

    /// <summary>
    /// The type is not supported by the client.
    /// </summary>
    /// <remarks>This value corresponds to <see cref="DbType.Time"/>.</remarks>
    Time = DbType.Time,

    /// <inheritdoc cref="DbType.UInt16"/>
    /// <remarks>This value corresponds to <see cref="DbType.UInt16"/>.</remarks>
    UInt16 = DbType.UInt16,

    /// <inheritdoc cref="DbType.UInt32"/>
    /// <remarks>This value corresponds to <see cref="DbType.UInt32"/>.</remarks>
    UInt32 = DbType.UInt32,

    /// <inheritdoc cref="DbType.UInt64"/>
    /// <remarks>This value corresponds to <see cref="DbType.UInt64"/>.</remarks>
    UInt64 = DbType.UInt64,

    /// <summary>
    /// A type representing a date and time value with time zone awareness.
    /// </summary>
    /// <remarks>This value corresponds to <see cref="DbType.DateTimeOffset"/>.</remarks>
    DateTimeOffset = DbType.DateTimeOffset,

}


