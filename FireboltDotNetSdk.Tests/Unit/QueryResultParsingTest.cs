using System.Numerics;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    public class QueryResultParsingTest
    {
        private const string InvalidFormatMessage = "Failed to execute a query. Invalid response body format. Try again or contact support.";

        private static object? ParseCell(string jsonValue)
        {
            var result = FireboltCommand.GetOriginalJsonData($"{{\"data\":[[{jsonValue}]]}}");
            return result!.Data[0][0];
        }

        [TestCase("0", 0L)]
        [TestCase("-1", -1L)]
        [TestCase("9223372036854775807", long.MaxValue)]
        [TestCase("-9223372036854775808", long.MinValue)]
        public void IntegersAreParsedAsLong(string json, long expected)
        {
            Assert.That(ParseCell(json), Is.TypeOf<long>().And.EqualTo(expected));
        }

        [TestCase("9223372036854775808")]
        [TestCase("-9223372036854775809")]
        [TestCase("123456789012345678901234567890")]
        public void IntegersBeyondLongAreParsedAsBigInteger(string json)
        {
            Assert.That(ParseCell(json), Is.TypeOf<BigInteger>().And.EqualTo(BigInteger.Parse(json)));
        }

        [TestCase("1.5", 1.5)]
        [TestCase("1e308", 1e308)]
        [TestCase("5e-324", 5e-324)]
        [TestCase("1.5e-5", 1.5e-5)]
        [TestCase("0.1000000000000000055511151231257827", 0.1)]
        [TestCase("3.141592653589793238462643383279", 3.141592653589793)]
        public void FractionalNumbersAreParsedAsDouble(string json, double expected)
        {
            Assert.That(ParseCell(json), Is.TypeOf<double>().And.EqualTo(expected));
        }

        [Test]
        public void NegativeZeroKeepsItsSign()
        {
            var value = ParseCell("-0.0");
            Assert.That(value, Is.TypeOf<double>());
            Assert.That(double.IsNegative((double)value!), Is.True);
        }

        [TestCase("\"abc\"", "abc")]
        [TestCase("\"inf\"", "inf")]
        [TestCase("\"-inf\"", "-inf")]
        [TestCase("\"nan\"", "nan")]
        [TestCase("\"\"", "")]
        [TestCase("\"\\u00e4\\u00f6 \\ud83d\\ude00\"", "äö 😀")]
        [TestCase("\"quote\\\" back\\\\slash \\n tab\\t\"", "quote\" back\\slash \n tab\t")]
        [TestCase("\"2023-10-24 16:21:42.123456+00\"", "2023-10-24 16:21:42.123456+00")]
        [TestCase("\"2023-10-24\"", "2023-10-24")]
        public void StringsAreParsedAsString(string json, string expected)
        {
            Assert.That(ParseCell(json), Is.TypeOf<string>().And.EqualTo(expected));
        }

        [TestCase("true", true)]
        [TestCase("false", false)]
        public void BooleansAreParsedAsBool(string json, bool expected)
        {
            Assert.That(ParseCell(json), Is.TypeOf<bool>().And.EqualTo(expected));
        }

        [Test]
        public void NullIsParsedAsNull()
        {
            Assert.That(ParseCell("null"), Is.Null);
        }

        // Newtonsoft turns ISO 8601 strings into DateTime by default; keep that for any string value that looks like one.
        [Test]
        public void IsoDateStringsAreParsedAsDateTime()
        {
            var unspecified = ParseCell("\"2023-10-24T16:21:42\"");
            Assert.That(unspecified, Is.TypeOf<DateTime>());
            Assert.That((DateTime)unspecified!, Is.EqualTo(new DateTime(2023, 10, 24, 16, 21, 42)));
            Assert.That(((DateTime)unspecified!).Kind, Is.EqualTo(DateTimeKind.Unspecified));

            var utc = ParseCell("\"2023-10-24T16:21:42.1234567Z\"");
            Assert.That(utc, Is.TypeOf<DateTime>());
            Assert.That(((DateTime)utc!).Kind, Is.EqualTo(DateTimeKind.Utc));
            Assert.That((DateTime)utc!, Is.EqualTo(new DateTime(2023, 10, 24, 16, 21, 42, DateTimeKind.Utc).AddTicks(1234567)));

            var offset = ParseCell("\"2023-10-24T16:21:42.5+02:00\"");
            Assert.That(offset, Is.TypeOf<DateTime>());
            Assert.That(((DateTime)offset!).Kind, Is.EqualTo(DateTimeKind.Local));
            Assert.That(((DateTime)offset!).ToUniversalTime(), Is.EqualTo(new DateTime(2023, 10, 24, 14, 21, 42, 500, DateTimeKind.Utc)));
        }

        [Test]
        public void NestedArraysAndObjectsAreParsedAsJTokens()
        {
            var array = ParseCell("[1, [2, \"x\"], null]");
            Assert.That(array, Is.TypeOf<JArray>());
            Assert.That(((JArray)array!).ToString(Formatting.None), Is.EqualTo("[1,[2,\"x\"],null]"));

            var obj = ParseCell("{\"a\": 1, \"b\": [true]}");
            Assert.That(obj, Is.TypeOf<JObject>());
            Assert.That(((JObject)obj!).ToString(Formatting.None), Is.EqualTo("{\"a\":1,\"b\":[true]}"));
        }

        [Test]
        public void AllFieldsAreParsed()
        {
            const string response = "{\"query\":{\"query_id\": \"16FDB86662938757\"},"
                + "\"meta\":[{\"name\": \"a\",\"type\": \"int\"}, {\"name\": \"b\",\"type\": \"text\"}],"
                + "\"data\":[[1, \"x\"], [2, null]],\"rows\": 2,"
                + "\"errors\":[{\"code\":\"E1\",\"description\":\"boom\",\"help_link\":\"http://help\",\"location\":{\"failing_line\":1,\"start_offset\":9}}],"
                + "\"statistics\":{\"elapsed\": 6.2e-4,\"rows_read\": 2,\"bytes_read\": 16,\"time_before_execution\": 0.000409657,"
                + "\"time_to_execute\": 0.000208377,\"scanned_bytes_cache\": 0,\"scanned_bytes_storage\": 9223372036854775807}}";

            var result = FireboltCommand.GetOriginalJsonData(response)!;

            Assert.Multiple(() =>
            {
                Assert.That(result.Query.QueryId, Is.EqualTo("16FDB86662938757"));
                Assert.That(result.Meta.Select(m => (m.Name, m.Type)), Is.EqualTo(new[] { ("a", "int"), ("b", "text") }));
                Assert.That(result.Data, Is.EqualTo(new[] { new object?[] { 1L, "x" }, new object?[] { 2L, null } }));
                Assert.That(result.Rows, Is.EqualTo(2));
                Assert.That(result.Errors.Single().Code, Is.EqualTo("E1"));
                Assert.That(result.Errors.Single().Description, Is.EqualTo("boom"));
                Assert.That(result.Errors.Single().HelpLink, Is.EqualTo("http://help"));
                Assert.That(result.Errors.Single().Location!.FailingLine, Is.EqualTo(1));
                Assert.That(result.Errors.Single().Location!.StartOffset, Is.EqualTo(9));
                Assert.That(result.Statistics.Elapsed, Is.EqualTo(6.2e-4));
                Assert.That(result.Statistics.RowsRead, Is.EqualTo(2));
                Assert.That(result.Statistics.BytesRead, Is.EqualTo(16));
                Assert.That(result.Statistics.TimeBeforeExecution, Is.EqualTo(0.000409657));
                Assert.That(result.Statistics.TimeToExecute, Is.EqualTo(0.000208377));
                Assert.That(result.Statistics.ScannedBytesCache, Is.EqualTo(0));
                Assert.That(result.Statistics.ScannedBytesStorage, Is.EqualTo(long.MaxValue));
            });
        }

        [Test]
        public void RepeatedKeysKeepTheLastValue()
        {
            const string response = "{\"data\":[[1]],\"meta\":[{\"name\":\"a\",\"type\":\"int\"}],\"errors\":[{\"code\":\"E1\"}],\"rows\":1,"
                + "\"statistics\":{\"elapsed\":1.0,\"elapsed\":2.0},"
                + "\"data\":[[2],[3]],\"meta\":[{\"name\":\"b\",\"type\":\"text\"}],\"errors\":[{\"code\":\"E2\"}],\"rows\":2}";

            var result = FireboltCommand.GetOriginalJsonData(response)!;

            Assert.Multiple(() =>
            {
                Assert.That(result.Data, Is.EqualTo(new[] { new object?[] { 2L }, new object?[] { 3L } }));
                Assert.That(result.Meta.Select(m => m.Name), Is.EqualTo(new[] { "b" }));
                Assert.That(result.Errors.Select(e => e.Code), Is.EqualTo(new[] { "E2" }));
                Assert.That(result.Rows, Is.EqualTo(2));
                Assert.That(result.Statistics.Elapsed, Is.EqualTo(2.0));
            });
        }

        [Test]
        public void RepeatedKeysInsideAnObjectCellKeepTheLastValue()
        {
            var cell = ParseCell("{\"a\": 1, \"a\": 2}");
            Assert.That(cell, Is.TypeOf<JObject>());
            Assert.That(((JObject)cell!).ToString(Formatting.None), Is.EqualTo("{\"a\":2}"));
        }

        [Test]
        public void MissingFieldsStayNull()
        {
            var result = FireboltCommand.GetOriginalJsonData("{}")!;
            Assert.Multiple(() =>
            {
                Assert.That(result.Query, Is.Null);
                Assert.That(result.Meta, Is.Null);
                Assert.That(result.Data, Is.Null);
                Assert.That(result.Rows, Is.Null);
                Assert.That(result.Errors, Is.Null);
                Assert.That(result.Statistics, Is.Null);
            });
        }

        [Test]
        public void EmptyDataIsParsedAsEmptyList()
        {
            Assert.That(FireboltCommand.GetOriginalJsonData("{\"data\":[]}")!.Data, Is.Empty);
        }

        [TestCase("null")]
        [TestCase(" null ")]
        public void JsonNullIsParsedAsNullResult(string response)
        {
            Assert.That(FireboltCommand.GetOriginalJsonData(response), Is.Null);
        }

        [Test]
        public void WhitespaceAroundJsonIsAccepted()
        {
            Assert.That(FireboltCommand.GetOriginalJsonData(" \n {\"rows\": 1} \n ")!.Rows, Is.EqualTo(1));
        }

        [Test]
        public void NullResponseIsRejected()
        {
            var exception = Assert.Throws<FireboltException>(() => FireboltCommand.GetOriginalJsonData(null));
            Assert.That(exception!.Message, Is.EqualTo("Response is empty while GetOriginalJSONData"));
        }

        [TestCase("")]
        [TestCase("   ")]
        [TestCase("not a json")]
        [TestCase("{\"rows\": 1")]
        [TestCase("{\"rows\": 1} trailing")]
        [TestCase("{\"rows\": 1}{\"rows\": 2}")]
        [TestCase("{\"data\": [[1, 2]")]
        [TestCase("{\"rows\": \"many\"")]
        [TestCase("[1, 2")]
        [TestCase("{\"data\": 1, ")]
        [TestCase("{\"errors\": [{\"location\": {\"failing_line\": \"x\"}}]}")]
        [TestCase("{\"errors\": [{\"location\": {\"failing_line\": 99999999999}}]}")]
        public void InvalidJsonIsRejectedWithFireboltException(string response)
        {
            var exception = Assert.Throws<FireboltException>(() => FireboltCommand.GetOriginalJsonData(response));
            Assert.That(exception!.Message, Is.EqualTo(InvalidFormatMessage));
            Assert.That(exception.InnerException, Is.TypeOf<JsonReaderException>());
        }

        [TestCase("/* */")]
        [TestCase("// only a comment")]
        [TestCase(" /* a */ // b\n ")]
        public void BodyWithOnlyCommentsIsRejectedWithFireboltException(string response)
        {
            var exception = Assert.Throws<FireboltException>(() => FireboltCommand.GetOriginalJsonData(response));
            Assert.That(exception!.Message, Is.EqualTo(InvalidFormatMessage));
            Assert.That(exception.InnerException, Is.TypeOf<JsonReaderException>());
        }

        [TestCase("[1, 2]")]
        [TestCase("\"text\"")]
        [TestCase("{\"data\": 1}")]
        [TestCase("{\"rows\": \"many\"}")]
        public void JsonOfTheWrongShapeIsRejectedWithFireboltException(string response)
        {
            var exception = Assert.Throws<FireboltException>(() => FireboltCommand.GetOriginalJsonData(response));
            Assert.That(exception!.Message, Is.EqualTo(InvalidFormatMessage));
            Assert.That(exception.InnerException, Is.TypeOf<JsonSerializationException>());
        }

        // The SDK must read results the same way whatever the host application configured globally.
        [Test]
        [NonParallelizable]
        public void GlobalJsonDefaultSettingsDoNotAffectParsing()
        {
            var previous = JsonConvert.DefaultSettings;
            JsonConvert.DefaultSettings = () => new JsonSerializerSettings
            {
                DateParseHandling = DateParseHandling.None,
                FloatParseHandling = FloatParseHandling.Decimal,
                MissingMemberHandling = MissingMemberHandling.Error,
                ObjectCreationHandling = ObjectCreationHandling.Reuse,
            };
            try
            {
                var result = FireboltCommand.GetOriginalJsonData("{\"unknown\": 1, \"data\": [[\"2023-10-24T16:21:42\", 1.5]], \"data\": [[\"2023-10-24T16:21:42\", 1.5]]}")!;
                Assert.Multiple(() =>
                {
                    Assert.That(result.Data, Has.Count.EqualTo(1));
                    Assert.That(result.Data[0][0], Is.TypeOf<DateTime>());
                    Assert.That(result.Data[0][1], Is.TypeOf<double>());
                });
            }
            finally
            {
                JsonConvert.DefaultSettings = previous;
            }
        }

        [Test]
        public void TypesConverterParsesLikeTheQueryPath()
        {
            const string response = "{\"data\":[[1]],\"rows\":1,\"data\":[[2]],\"rows\":2}";
            var fromConverter = TypesConverter.ParseJsonResponse(response)!;
            var fromQueryPath = FireboltCommand.GetOriginalJsonData(response)!;
            Assert.Multiple(() =>
            {
                Assert.That(fromConverter.Data, Is.EqualTo(new[] { new object?[] { 2L } }));
                Assert.That(fromConverter.Data, Is.EqualTo(fromQueryPath.Data));
                Assert.That(fromConverter.Rows, Is.EqualTo(fromQueryPath.Rows));
            });

            var exception = Assert.Throws<FireboltException>(() => TypesConverter.ParseJsonResponse("/* */"));
            Assert.That(exception!.Message, Is.EqualTo("Error while parsing response"));
            Assert.That(exception.InnerException, Is.TypeOf<JsonReaderException>());
        }
    }
}
