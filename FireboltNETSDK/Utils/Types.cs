/*
 * Copyright (c) 2022 FireBolt All rights reserved.
 */

using System.Collections;
using System.Globalization;
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
        Boolean, Array, Short, ByteA, Geography, Struct
    }
    public static class TypesConverter
    {

        //Regex that matches the string Nullable(<type>), where type is the type that we need to capture.
        private const string NullableTypePattern = @"Nullable\(([^)]+)\)";
        private const int matchTimeoutSeconds = 60;
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

        public static object? ConvertToCSharpVal(object? val, ColumnType columnType)
        {
            if (val == null)
            {
                return null;
            }
            if (val is JContainer jContainer)
            {
                val = jContainer.ToString();
            }

            return columnType switch
            {
                ArrayType arrayType => ToArray(val, arrayType),
                StructType structType => ToStruct(val, structType),
                _ => columnType.Type switch
                {
                    FireboltDataType.Long => Convert.ToInt64(val),
                    FireboltDataType.Int => Convert.ToInt32(val),
                    FireboltDataType.Decimal => Convert.ToDecimal(val),
                    FireboltDataType.String => val.ToString(),
                    FireboltDataType.Geography => val.ToString(),
                    FireboltDataType.DateTime => ParseDateTime(val),
                    FireboltDataType.TimestampTz => ParseDateTime(val),
                    FireboltDataType.TimestampNtz => ParseDateTime(val),
                    FireboltDataType.Date => ParseDateTime(val),
                    FireboltDataType.Short => Convert.ToInt16(val),
                    FireboltDataType.Double => ParseDouble(val),
                    FireboltDataType.Float => ParseFloat(val),
                    FireboltDataType.Boolean => ParseBoolean(val),
                    FireboltDataType.ByteA => val switch
                    {
                        string str => Convert.FromHexString(str.Remove(0, 2)),
                        _ => throw new FireboltException("Unexpected bytea value type: " + val.GetType())
                    },
                    FireboltDataType.Null => throw new FireboltException("Not null value in null type"),
                    _ => throw new FireboltException("Invalid destination type: " + columnType.Type)
                }
            };
        }

        public static Type GetType(ColumnType columnType)
        {
            if (columnType is ArrayType arrayType)
            {
                return GetArrayType(arrayType);
            }
            var normalType = columnType switch
            {
                StructType => typeof(Dictionary<string, object>),
                _ => columnType.Type switch
                {
                    FireboltDataType.Long => typeof(long),
                    FireboltDataType.Int => typeof(int),
                    FireboltDataType.Decimal => typeof(decimal),
                    FireboltDataType.String => typeof(string),
                    FireboltDataType.Geography => typeof(string),
                    FireboltDataType.DateTime => typeof(DateTime),
                    FireboltDataType.TimestampTz => typeof(DateTime),
                    FireboltDataType.TimestampNtz => typeof(DateTime),
                    FireboltDataType.Date => typeof(DateTime),
                    FireboltDataType.Short => typeof(short),
                    FireboltDataType.Double => typeof(double),
                    FireboltDataType.Float => typeof(float),
                    FireboltDataType.Boolean => typeof(bool),
                    FireboltDataType.ByteA => columnType.Nullable ? typeof(byte?[]) : typeof(byte[]),
                    FireboltDataType.Null => throw new FireboltException("Not null value in null type"),
                    _ => throw new FireboltException("Invalid destination type: " + columnType.Type)
                }
            };
            if (normalType == typeof(string)
                || normalType == typeof(byte?[])
                || normalType == typeof(byte[])
                || !columnType.Nullable)
            {
                return normalType;
            }
            return typeof(Nullable<>).MakeGenericType(normalType);
        }

        public static Type GetArrayType(ArrayType arrayType)
        {
            if (arrayType.InnerType is ArrayType innerArrayType)
            {
                return GetArrayType(innerArrayType).MakeArrayType();
            }

            return GetType(arrayType.InnerType).MakeArrayType();
        }

        public static bool ParseBoolean(object val)
        {
            return val switch
            {
                string str when booleanValues.TryGetValue(str, out bool result) => result,
                string str => throw new FormatException($"String '{str}' was not recognized as a valid Boolean."),
                bool b => b,
                int i => i != 0,
                _ => throw new FireboltException("Unexpected value type: " + val.GetType())
            };
        }

        public static DateTime ParseDateTime(object val)
        {
            if (val is string str)
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
                    var pattern =
                        OffsetDateTimePattern.CreateWithInvariantCulture("yyyy-MM-dd HH:mm:ss.FFFFFFo<+HH:mm:ss>");
                    return pattern.Parse(str).Value.InFixedZone().ToDateTimeUtc().ToLocalTime();
                }
            }
            throw new FireboltException("Unexpected datetime value type: " + val.GetType());
        }

        private static double ParseDouble(object val)
        {
            return val switch
            {
                string str when doubleInfinity.ContainsKey(str) => doubleInfinity[str],
                double d => d,
                _ => Convert.ToDouble(val)
            };
        }

        private static float ParseFloat(object val)
        {
            return val switch
            {
                string str when floatInfinity.ContainsKey(str) => floatInfinity[str],
                float f => f,
                _ => Convert.ToSingle(val)
            };
        }

        private static Array? ToArray(object val, ArrayType arrayType)
        {
            var type = GetType(arrayType);
            return val switch
            {
                string str => JsonConvert.DeserializeObject(str, type) as Array,
                _ => throw new FireboltException("Unexpected array value type: " + val.GetType())
            };
        }

        private static Dictionary<string, object?>? ToStruct(object val, StructType structType)
        {
            return val switch
            {
                string str => ConvertDict(JsonConvert.DeserializeObject<Dictionary<string, object>>(str)),
                _ => throw new FireboltException("Unexpected struct value type: " + val.GetType())
            };
            Dictionary<string, object?>? ConvertDict(Dictionary<string, object>? dict) => dict?.ToDictionary(x => x.Key, x => ConvertToCSharpVal(x.Value, structType.Fields[x.Key]));
        }

        public static FireboltDataType MapColumnTypeToFireboltDataType(string columnType)
        {
            var csharpType = columnType.ToLower() switch
            {
                "string" => FireboltDataType.String,
                "long" => FireboltDataType.Long,
                "bigint" => FireboltDataType.Long,
                "short" => FireboltDataType.Short,
                "int" => FireboltDataType.Int,
                "integer" => FireboltDataType.Int,
                "float" => FireboltDataType.Float,
                "real" => FireboltDataType.Float,
                "double" => FireboltDataType.Double,
                "double precision" => FireboltDataType.Double,
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
                "geography" => FireboltDataType.Geography,
                "struct" => FireboltDataType.Struct,
                _ => throw new FireboltException("The data type returned from the server is not supported: " + columnType),
            };
            return csharpType;
        }

        public static string GetFullColumnTypeName(Meta meta)
        {
            Match nullableMatch = Regex.Match(meta.Type, NullableTypePattern, RegexOptions.None, TimeSpan.FromSeconds(matchTimeoutSeconds));
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
