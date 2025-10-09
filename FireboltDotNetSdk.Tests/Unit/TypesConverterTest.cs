using System.Text;
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;
using Newtonsoft.Json.Linq;
using NodaTime.Text;

namespace FireboltDotNetSdk.Tests;

public class TypesConverterTest
{
    private const string WRONG_NUMERIC_FORMAT_ERROR_MESSAGE = "Input string was not in a correct format.";

    [TestCase("string", "Hello", "Hello")]
    [TestCase("string", null, null)]
    [TestCase("text", "Hello", "Hello")]
    [TestCase("string", null, null)]
    [TestCase("null", null, null)]
    [TestCase("nothing", null, null)]
    [TestCase("nullable", null, null)]
    [TestCase("float", "1.5", 1.5f)]
    [TestCase("float", 1.5f, 1.5f)]
    [TestCase("long", "5555555", 5555555)]
    [TestCase("long", 5555555, 5555555)]
    [TestCase("int", "12345", 12345)]
    [TestCase("int", 12345, 12345)]
    [TestCase("integer", "12345", 12345)]
    [TestCase("integer", 12345, 12345)]
    [TestCase("short", "1234", 1234)]
    [TestCase("short", 1234, 1234)]
    [TestCase("double", "15.18", 15.18d)]
    [TestCase("double", 15.18d, 15.18d)]
    [TestCase("decimal(29,30)", "15.9999999999999999999999999999", 16)]
    [TestCase("decimal(29,30)", "15.5400000000000000000000000000", 15.54)]
    [TestCase("decimal(29,30)", "32", 32)]
    [TestCase("decimal(29,30)", "-15.5400000000000000000000000000", -15.54)]
    [TestCase("decimal(29,30)", "-32", -32)]
    [TestCase("decimal(29,30)", "-0", 0)]
    [TestCase("decimal(29,30)", "0", 0)]
    [TestCase("decimal(29,30)", "0.321", 0.321)]
    [TestCase("decimal(29,30)", "12345678901234567890", 12345678901234567890)] // number longer thant 19 characters
    [TestCase("decimal(29,30)", 123.456, 123.456)]
    [TestCase("timestampntz", null, null)]
    [TestCase("timestamptz", null, null)]
    [TestCase("pgdate", null, null)]
    [TestCase("date_ext", null, null)]
    [TestCase("float", "inf", float.PositiveInfinity)]
    [TestCase("float", "+inf", float.PositiveInfinity)]
    [TestCase("float", "-inf", float.NegativeInfinity)]
    [TestCase("float", "nan", float.NaN)]
    [TestCase("float", "-nan", float.NaN)]
    [TestCase("double", "inf", double.PositiveInfinity)]
    [TestCase("double", "+inf", double.PositiveInfinity)]
    [TestCase("double", "-inf", double.NegativeInfinity)]
    [TestCase("double", "nan", double.NaN)]
    [TestCase("double", "-nan", double.NaN)]
    [TestCase("boolean", "true", true)]
    [TestCase("boolean", "false", false)]
    [TestCase("boolean", "1", true)]
    [TestCase("boolean", "0", false)]
    [TestCase("boolean", true, true)]
    [TestCase("boolean", false, false)]
    [TestCase("boolean", 1, true)]
    [TestCase("boolean", 0, false)]
    public void ConvertProvidedValues(string columnTypeName, object value, object expectedValue)
    {
        ColumnType columnType = ColumnType.Of(columnTypeName);
        object? result = TypesConverter.ConvertToCSharpVal(value, columnType);
        Assert.That(expectedValue, Is.EqualTo(result));
    }

    [TestCase("int", "+inf", WRONG_NUMERIC_FORMAT_ERROR_MESSAGE)]
    [TestCase("int", "-inf", WRONG_NUMERIC_FORMAT_ERROR_MESSAGE)]
    [TestCase("int", "inf", WRONG_NUMERIC_FORMAT_ERROR_MESSAGE)]
    [TestCase("long", "+inf", WRONG_NUMERIC_FORMAT_ERROR_MESSAGE)]
    [TestCase("long", "-inf", WRONG_NUMERIC_FORMAT_ERROR_MESSAGE)]
    [TestCase("long", "inf", WRONG_NUMERIC_FORMAT_ERROR_MESSAGE)]
    [TestCase("int", "hello", WRONG_NUMERIC_FORMAT_ERROR_MESSAGE)]
    [TestCase("int", "bye", WRONG_NUMERIC_FORMAT_ERROR_MESSAGE)]
    [TestCase("int", "not a number", WRONG_NUMERIC_FORMAT_ERROR_MESSAGE)]
    [TestCase("long", "some text", WRONG_NUMERIC_FORMAT_ERROR_MESSAGE)]
    [TestCase("long", "", WRONG_NUMERIC_FORMAT_ERROR_MESSAGE)]
    [TestCase("long", "not empty text", WRONG_NUMERIC_FORMAT_ERROR_MESSAGE)]
    [TestCase("boolean", "2", "String '2' was not recognized as a valid Boolean.")]
    [TestCase("boolean", "t", "String 't' was not recognized as a valid Boolean.")]
    [TestCase("boolean", "yes", "String 'yes' was not recognized as a valid Boolean.")]
    public void FailingConvertProvidedValues(string columnTypeName, string value, string expectedErrorMessage)
    {
        ColumnType columnType = ColumnType.Of(columnTypeName);
        FormatException? e = Assert.Throws<FormatException>(() => TypesConverter.ConvertToCSharpVal(value, columnType));
        string visualTypeName = char.ToUpper(columnTypeName[0]) + columnTypeName.Substring(1);
        Assert.That(e?.Message, Is.EqualTo(expectedErrorMessage));
    }

    [Test]
    public void ConvertTimestampTz()
    {
        ColumnType columnType = ColumnType.Of("TimestampTz");
        var value = "2022-05-10 21:01:02.12345+00";
        DateTime expectedValue = DateTime.Parse("2022-05-10 21:01:02.12345+00");
        object? result = TypesConverter.ConvertToCSharpVal(value, columnType);
        Assert.That(expectedValue, Is.EqualTo(result));
    }

    [Test]
    public void ConvertTimestampNtz()
    {
        ColumnType columnType = ColumnType.Of("TimestampNtz");
        var value = "2022-05-10 23:01:02.123456";
        DateTime expectedValue = new DateTime(2022, 5, 10, 23, 1, 2, 0);
        expectedValue = expectedValue.AddTicks(1234560);
        object? result = TypesConverter.ConvertToCSharpVal(value, columnType);
        Assert.That(result, Is.EqualTo(expectedValue));
    }

    [TestCase("timestamp")]
    [TestCase("timestamp_ext")]
    [TestCase("datetime")]
    [TestCase("date")]
    [TestCase("date_ext")]
    [TestCase("pgDate")]
    public void ConvertTimestamp(string type)
    {
        ColumnType columnType = ColumnType.Of(type);
        var value = "2022-05-10 23:01:02";
        DateTime expectedValue = new DateTime(2022, 5, 10, 23, 1, 2, 0);
        object? result = TypesConverter.ConvertToCSharpVal(value, columnType);
        Assert.That(result, Is.EqualTo(expectedValue));
    }

    [TestCase("date")]
    [TestCase("date_ext")]
    [TestCase("pgDate")]
    [TestCase("TimestampTz")]
    [TestCase("TimestampNtz")]
    public void ConvertNotDateToDate(string type)
    {
        ColumnType columnType = ColumnType.Of(type);
        var value = "something that is not date";
        Assert.Throws<UnparsableValueException>(() => TypesConverter.ConvertToCSharpVal(value, columnType));
    }

    [Test]
    public void ConvertInvalidValue()
    {
        ColumnType columnType = ColumnType.Of("integer");
        var value = "hello";
        FormatException? exception = Assert.Throws<FormatException>(() => TypesConverter.ConvertToCSharpVal(value, columnType));
        Assert.That(exception, Is.Not.Null);
        Assert.That(exception!.Message, Is.EqualTo("Input string was not in a correct format."));
    }

    public static IEnumerable<TestCaseData> ArrayTestCases()
    {
        yield return new TestCaseData(JArray.Parse("[1, 2]"), new[] { 1, 2 }, "array(int)");
        yield return new TestCaseData("[1, 2]", new[] { 1, 2 }, "array(int)");
        yield return new TestCaseData("[\"1\", \"2\"]", new[] { 1, 2 }, "array(int)");
        yield return new TestCaseData("[\"2022-05-10\", \"2022-05-11\"]", new[] { DateTime.Parse("2022-05-10"), DateTime.Parse("2022-05-11") }, "array(date)");
        yield return new TestCaseData("[\"2022-05-10 23:01:02\", \"2022-05-11 23:01:02\"]", new[] { DateTime.Parse("2022-05-10 23:01:02"), DateTime.Parse("2022-05-11 23:01:02") }, "array(timestamp)");
        yield return new TestCaseData("[[1, 2], [3, 4]]", new[] { new[] { 1, 2 }, new[] { 3, 4 } }, "array(array(int))");
    }

    [TestCaseSource(nameof(ArrayTestCases))]
    public void ConvertArray(object arrayValue, object expected, string type)
    {
        ColumnType columnType = ColumnType.Of(type);
        object? result = TypesConverter.ConvertToCSharpVal(arrayValue, columnType);
        Assert.That(result, Is.EqualTo(expected));
    }

    public static IEnumerable<TestCaseData> StructTestCase()
    {
        yield return new TestCaseData(
            JObject.Parse("{\"a\": 1, \"b\": \"value\"}"),
            new Dictionary<String, object> { { "a", 1 }, { "b", "value" } },
            "struct(a int, b string)"
        );
        yield return new TestCaseData(
            "{\"a\": 1, \"b\": \"value\"}",
            new Dictionary<String, object> { { "a", 1 }, { "b", "value" } },
            "struct(a int, b string)"
        );

        yield return new TestCaseData(
            "{\"a\": 1, \"b\": {\"c\": 2}}",
            new Dictionary<String, object> { { "a", 1 }, { "b", new Dictionary<String, object> { { "c", 2 } } } },
            "struct(a int, b struct(c int))"
        );
        yield return new TestCaseData(
            "{\"a\": 1, \"b\": {\"c\": [1, 2]}}",
            new Dictionary<String, object> { { "a", 1 }, { "b", new Dictionary<String, object> { { "c", new[] { 1, 2 } } } } },
            "struct(a int, b struct(c array(int)))"
        );
        yield return new TestCaseData(
            "{\"a\": 1, \"b\": {\"c\": \"2022-05-10 23:01:02\"}}",
            new Dictionary<String, object> { { "a", 1 }, { "b", new Dictionary<String, object> { { "c", DateTime.Parse("2022-05-10 23:01:02") } } } },
            "struct(a int, b struct(c timestamp))"
        );
        yield return new TestCaseData(
            "{\"a\": 1, \"b\": [{\"c\": 2}, {\"c\": 3}]}",
            new Dictionary<String, object> { { "a", 1 }, { "b", new[] { new Dictionary<String, object> { { "c", 2 } }, new Dictionary<String, object> { { "c", 3 } } } } },
            "struct(a int, b array(struct(c int)))"
        );
        yield return new TestCaseData(
            "{\"a\": 1, \"b\": null}",
            new Dictionary<String, object?> { { "a", 1 }, { "b", null } },
            "struct(a int, b int null)"
        );
        yield return new TestCaseData(
            "{\"a\": 1, \"b c\": \"value\"}",
            new Dictionary<String, object> { { "a", 1 }, { "b c", "value" } },
            "struct(a int, `b c` string)"
        );
    }

    [TestCaseSource(nameof(StructTestCase))]
    public void ConvertStruct(object structValue, object expected, string type)
    {
        ColumnType columnType = ColumnType.Of(type);
        object? result = TypesConverter.ConvertToCSharpVal(structValue, columnType);
        Assert.That(result, Is.EqualTo(expected));
    }

    [TestCase(null, "Response is empty", "Response is empty")]
    [TestCase("invalid response", "Error while parsing response", "Unexpected character encountered while parsing value: i. Path '', line 0, position 0.")]
    public void ParseWrongJsonResponse(string? json, string? expectedMessage, string? expectedBaseMessage)
    {
        FireboltException? exception = Assert.Throws<FireboltException>(() => TypesConverter.ParseJsonResponse(json));
        Assert.That(exception?.Message, Is.EqualTo(expectedMessage));
        Assert.That(exception?.GetBaseException().Message, Is.EqualTo(expectedBaseMessage));
    }

    [Test]
    public void ParseNullJsonResponse()
    {
        Assert.Null(TypesConverter.ParseJsonResponse("null"));
    }

    [Test]
    public void ParseJsonResponseWithNewTypes()
    {
        var responseWithNewTypes =
                "{\"query\":{\"query_id\": \"174005F13D908A5D\"},\"meta\":[{\"name\": \"uint8\",\"type\": \"int\"},{\"name\": \"int_8\",\"type\": \"int\"},{\"name\": \"uint16\",\"type\": \"int\"},{\"name\": \"int16\",\"type\": \"int\"},{\"name\": \"uint32\",\"type\": \"int\"},{\"name\": \"int32\",\"type\": \"int\"},{\"name\": \"uint64\",\"type\": \"long\"},{\"name\": \"int64\",\"type\": \"long\"},{\"name\": \"float32\",\"type\": \"float\"},{\"name\": \"float64\",\"type\": \"double\"},{\"name\": \"string\",\"type\": \"text\"},{\"name\": \"date\",\"type\": \"date\"},{\"name\": \"array\",\"type\": \"array(int)\"},{\"name\": \"decimal\",\"type\": \"Decimal(38, 30)\"},{\"name\": \"nullable\",\"type\": \"int null\"}],\"data\":[[1, -1, 257, -257, 80000, -80000, 30000000000, -30000000000, 1.23, 1.23456789012, \"text\", \"2021-03-28\", [1,2,3,4], 1231232.123459999990457054844258706536, null]],\"rows\": 1,\"statistics\":{\"elapsed\": 0.001662899,\"rows_read\": 1,\"bytes_read\": 1,\"time_before_execution\": 0.001246457,\"time_to_execute\": 0.000166576,\"scanned_bytes_cache\": 0,\"scanned_bytes_storage\": 0}}"
            ;
        QueryResult res = TypesConverter.ParseJsonResponse(responseWithNewTypes)!;
        object?[] expected =
        {
            1, -1, 257, -257, 80000, -80000, 30000000000, -30000000000, 1.23f, 1.23456789012, "text",
            DateTime.Parse("2021-03-28"), new [] { 1, 2, 3, 4 }, 1231232.123459999990457054844258706536, null
        };

        for (int i = 0; i < expected.Length; i++)
        {
            AssertQueryResult(res, expected[i], 0, i);
        }

        Assert.NotNull(res);
    }

    private void AssertQueryResult(QueryResult result, object? expectedValue, int line = 0, int column = 0)
    {
        var columnType = ColumnType.Of(TypesConverter.GetFullColumnTypeName(result.Meta[column]));
        var convertedValue = TypesConverter.ConvertToCSharpVal(result.Data[line][column], columnType);
        Assert.That(convertedValue, Is.EqualTo(expectedValue));
    }

    [Test]
    public void ConvertValidHexStringToByteA()
    {
        ColumnType columnType = ColumnType.Of("ByteA");
        var expectedBytes = Encoding.ASCII.GetBytes("hello_world_123");
        object? result = TypesConverter.ConvertToCSharpVal("\\x68656c6c6f5f776f726c645f313233", columnType);
        Assert.That(result, Is.EqualTo(expectedBytes));
    }

    [Test]
    public void ConvertInvalidHexStringToByteA()
    {
        ColumnType columnType = ColumnType.Of("ByteA");
        //invalid because a valid hex string contains an even number of digits;
        var invalidHexString = "\\x686";
        Assert.Throws<FormatException>(() => TypesConverter.ConvertToCSharpVal(invalidHexString, columnType));
    }

    [Test]
    public void ConvertByteAWhenNull()
    {
        ColumnType columnType = ColumnType.Of("ByteA");
        Assert.Null(TypesConverter.ConvertToCSharpVal(null, columnType));
    }

    [Test]
    public void ConvertNotNullValueOfNullType()
    {
        Assert.That(Assert.Throws<FireboltException>(() => TypesConverter.ConvertToCSharpVal("hello", ColumnType.Of("null")))?.GetBaseException().Message, Is.EqualTo("Not null value in null type"));
    }

    [Test]
    public void ConvertWrongType()
    {
        Assert.That(Assert.Throws<FireboltException>(() => TypesConverter.ConvertToCSharpVal("hello", ColumnType.Of("something wrong")))?.GetBaseException().Message, Is.EqualTo("The data type returned from the server is not supported: something wrong"));
    }
}