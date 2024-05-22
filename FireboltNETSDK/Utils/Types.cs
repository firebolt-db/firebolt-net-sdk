/*
 * Copyright (c) 2022 FireBolt All rights reserved.
 */

using System.Globalization;
using System.Text.RegularExpressions;
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
        // when ParseBoolean is called from ConvertToCSharpVal that in turn invoked from FireboltDataReader.GetValueSafely() that calls ToString()
        // to create string value representation. In case of boolean ToString() performs capitalization,
        // so true becomes string True and false becomes False. However when ParseBoolean is invoked directly from GetBoolean() 
        // the value may be true (lower case). This is the reason to use case insensitive comparison. 
        private static IDictionary<string, bool> booleanValues = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase)
        {
            { "true", true },
            { "false", false },
            { "1", true },
            { "0", false },
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
                    return Convert.ToInt64(str.Trim('"'));
                case FireboltDataType.Int:
                    return Convert.ToInt32(str.Trim('"'));
                case FireboltDataType.Decimal:
                    return Convert.ToDecimal(str.Trim('"'));
                case FireboltDataType.String:
                    return str;
                case FireboltDataType.DateTime:
                case FireboltDataType.TimestampTz:
                case FireboltDataType.TimestampNtz:
                case FireboltDataType.Date:
                    return ParseDateTime(str);
                case FireboltDataType.Short:
                    return Convert.ToInt16(str.Trim('"'));
                case FireboltDataType.Double:
                    return doubleInfinity.ContainsKey(str) ? doubleInfinity[str] : Convert.ToDouble(str.Trim('"'), CultureInfo.InvariantCulture);
                case FireboltDataType.Float:
                    return floatInfinity.ContainsKey(str) ? floatInfinity[str] : Convert.ToSingle(str.Trim('"'), CultureInfo.InvariantCulture);
                case FireboltDataType.Boolean:
                    return ParseBoolean(str);
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

        public static bool ParseBoolean(string str)
        {
            bool b;
            return booleanValues.TryGetValue(str, out b) ? b : throw new FormatException($"String '{str}' was not recognized as a valid Boolean.");
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

        public static QueryResult? ParseJsonResponse(string? response)
        {
            if (response == null)
            {
                throw new FireboltException("Response is empty");
            }
            try
            {
                var prettyJson = JToken.Parse(response).ToString(Formatting.Indented);
                return JsonConvert.DeserializeObject<QueryResult>(prettyJson);
            }
            catch (Exception e)
            {
                throw new FireboltException("Error while parsing response", e);
            }
        }

        public static bool isInfinity(object value)
        {
            return infinityValues.Contains(value);
        }

        public static bool isNaN(object value)
        {
            // NaN values should not be compared with ==, so we use IsNaN
            return value is double d && double.IsNaN(d) || value is float f && float.IsNaN(f);
        }
    }
}
