using FireboltDoNetSdk.Utils;
using FireboltDotNetSdk.Utils;

namespace FireboltDotNetSdk.Tests;

public class TypesConverterTest
{
    [TestCase("string", "Hello", "Hello")]
    [TestCase("string", null, null)]
    [TestCase("text", "Hello", "Hello")]
    [TestCase("string", null, null)]
    [TestCase("null", null, null)]
    [TestCase("nothing", null, null)]
    [TestCase("nullable", null, null)]
    [TestCase("float", "1.5", 1.5f)]
    [TestCase("array(integer null) null", "[1,2]", new[] { 1, 2 })]
    [TestCase("long", "5555555", 5555555)]
    [TestCase("int", "12345", 12345)]
    [TestCase("integer", "12345", 12345)]
    [TestCase("double", "15.18", 15.18d)]
    [TestCase("decimal(29,30)", "15.9999999999999999999999999999", 16)]
    [TestCase("decimal(29,30)", "15.5400000000000000000000000000", 15.54)]
    [TestCase("timestampntz", null, null)]
    [TestCase("timestamptz", null, null)]
    [TestCase("pgdate", null, null)]
    [TestCase("date_ext", null, null)]
    public void ConvertProvidedValues(String columnTypeName, string value, object expectedValue)
    {
        ColumnType columnType = ColumnType.Of(columnTypeName);
        object result = TypesConverter.ConvertToCSharpVal(value, columnType);
        Assert.That(expectedValue, Is.EqualTo(result));
    }

    [Test]
    public void ConvertTimestampTz()
    {
        ColumnType columnType = ColumnType.Of("TimestampTz");
        var value = "2022-05-10 21:01:02.12345+00";
        DateTime expectedValue = DateTime.Parse("2022-05-10 21:01:02.12345+00");
        object result = TypesConverter.ConvertToCSharpVal(value, columnType);
        Assert.That(expectedValue, Is.EqualTo(result));
    }

    [Test]
    public void ConvertTimestampNtz()
    {
        ColumnType columnType = ColumnType.Of("TimestampNtz");
        var value = "2022-05-10 23:01:02.123456";
        DateTime expectedValue = new DateTime(2022, 5, 10, 23, 1, 2, 0);
        expectedValue = expectedValue.AddTicks(1234560);
        object result = TypesConverter.ConvertToCSharpVal(value, columnType);
        Assert.That(result, Is.EqualTo(expectedValue));
    }

    [Test]
    [TestCase("date")]
    [TestCase("date_ext")]
    [TestCase("pgDate")]
    public void ConvertPgDate(string type)
    {
        ColumnType columnType = ColumnType.Of(type);
        var value = "2022-05-10";
        object result = TypesConverter.ConvertToCSharpVal(value, columnType);
        DateOnly expectedDate = DateOnly.FromDateTime(new DateTime(2022, 5, 10, 23, 1, 2, 0));
        Assert.That(result, Is.EqualTo(expectedDate));
    }
}