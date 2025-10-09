using FireboltDotNetSdk.Utils;

namespace FireboltDotNetSdk.Tests;

public class ColumnTypeTest
{
    [Test]
    public void CreateColumnTypeWithArrayTypeTest()
    {
        String type = "array(integer NULL)";
        ColumnType columnType = ColumnType.Of(type);
        if (columnType is not ArrayType arrayType)
        {
            Assert.Fail("ArrayType expected");
            return;
        }

        Assert.That(arrayType.Type, Is.EqualTo(FireboltDataType.Array));
        Assert.False(arrayType.Nullable);
        Assert.Null(arrayType.Precision);
        Assert.Null(arrayType.Scale);
        Assert.NotNull(arrayType.InnerType);
        Assert.True(arrayType.InnerType!.Nullable);
        Assert.That(arrayType.InnerType.Type, Is.EqualTo(FireboltDataType.Int));
    }

    [Test]
    public void CreateColumnTypeWithNullableArrayTypeTest()
    {
        String type = "array(integer NULL) NULL";
        ColumnType columnType = ColumnType.Of(type);
        if (columnType is not ArrayType arrayType)
        {
            Assert.Fail("ArrayType expected");
            return;
        }

        Assert.That(arrayType.Type, Is.EqualTo(FireboltDataType.Array));
        Assert.True(arrayType.Nullable);
        Assert.Null(arrayType.Precision);
        Assert.Null(arrayType.Scale);
        Assert.NotNull(arrayType.InnerType);
        Assert.True(arrayType.InnerType!.Nullable);
        Assert.That(arrayType.InnerType.Type, Is.EqualTo(FireboltDataType.Int));
    }

    [Test]
    public void CreateColumnTypeWithNullableDecimalTest()
    {
        String type = "decimal(5,2) null";
        ColumnType columnType = ColumnType.Of(type);
        Assert.That(columnType.Type, Is.EqualTo(FireboltDataType.Decimal));
        Assert.That(columnType.Precision, Is.EqualTo(5));
        Assert.That(columnType.Scale, Is.EqualTo(2));
    }

    [Test]
    public void CreateColumnTypeWithNullableArrayOfArrayTypeTest()
    {
        String type = "array(array(decimal(5,2) null) null)";
        ColumnType columnType = ColumnType.Of(type);
        if (columnType is not ArrayType arrayType)
        {
            Assert.Fail("ArrayType expected");
            return;
        }

        Assert.That(arrayType.Type, Is.EqualTo(FireboltDataType.Array));
        Assert.False(arrayType.Nullable);
        Assert.Null(arrayType.Precision);
        Assert.Null(arrayType.Scale);
        Assert.NotNull(arrayType.InnerType);
        Assert.True(arrayType.InnerType!.Nullable);
        Assert.That(arrayType.InnerType.Type, Is.EqualTo(FireboltDataType.Array));

        if (arrayType.InnerType is not ArrayType innerArrayType)
        {
            Assert.Fail("ArrayType expected");
            return;
        }
        Assert.NotNull(innerArrayType.InnerType);
        Assert.True(innerArrayType.InnerType!.Nullable);
        Assert.That(innerArrayType.InnerType.Type, Is.EqualTo(FireboltDataType.Decimal));
        Assert.That(innerArrayType.InnerType.Precision, Is.EqualTo(5));
        Assert.That(innerArrayType.InnerType.Scale, Is.EqualTo(2));
    }

    [TestCase("string NULL", FireboltDataType.String, true)]
    [TestCase("string", FireboltDataType.String, false)]
    [TestCase("text", FireboltDataType.String, false)]
    [TestCase("text null", FireboltDataType.String, true)]
    [TestCase("date", FireboltDataType.Date, false)]
    [TestCase("date_ext", FireboltDataType.Date, false)]
    [TestCase("date null", FireboltDataType.Date, true)]
    [TestCase("date_ext null", FireboltDataType.Date, true)]
    [TestCase("null", FireboltDataType.Null, true)]
    [TestCase("null null", FireboltDataType.Null, true)]
    [TestCase("nothing", FireboltDataType.Null, true)]
    [TestCase("nothing null", FireboltDataType.Null, true)]
    [TestCase("nullable", FireboltDataType.Null, true)]
    [TestCase("nullable null", FireboltDataType.Null, true)]
    [TestCase("timestamptz", FireboltDataType.TimestampTz, false)]
    [TestCase("timestamptz null", FireboltDataType.TimestampTz, true)]
    [TestCase("timestampntz NULL", FireboltDataType.TimestampNtz, true)]
    [TestCase("timestampntz", FireboltDataType.TimestampNtz, false)]
    [TestCase("pgdate null", FireboltDataType.Date, true)]
    [TestCase("pgdate", FireboltDataType.Date, false)]
    [TestCase("float", FireboltDataType.Float, false)]
    [TestCase("float null", FireboltDataType.Float, true)]
    [TestCase("array(integer null) null", FireboltDataType.Array, true)]
    [TestCase("array(integer null)", FireboltDataType.Array, false)]
    [TestCase("long", FireboltDataType.Long, false)]
    [TestCase("long null", FireboltDataType.Long, true)]
    [TestCase("int", FireboltDataType.Int, false)]
    [TestCase("int null", FireboltDataType.Int, true)]
    [TestCase("integer", FireboltDataType.Int, false)]
    [TestCase("integer null", FireboltDataType.Int, true)]
    [TestCase("double null", FireboltDataType.Double, true)]
    [TestCase("double", FireboltDataType.Double, false)]
    [TestCase("geography", FireboltDataType.Geography, false)]
    [TestCase("geography null", FireboltDataType.Geography, true)]
    public void CreateColumnTypeWithProvidedColumnTypeNamesTest(String columnTypeName, FireboltDataType expectedType, bool expectedIsNullable)
    {
        ColumnType columnType = ColumnType.Of(columnTypeName);
        Assert.That(columnType.Type, Is.EqualTo(expectedType));
        Assert.That(columnType.Nullable, Is.EqualTo(expectedIsNullable));
        Assert.Null(columnType.Precision);
        Assert.Null(columnType.Scale);
    }

    [Test]
    public void CreateStructTypeTest()
    {
        String type = "struct(a int, b text)";
        ColumnType columnType = ColumnType.Of(type);
        Assert.That(columnType.Type, Is.EqualTo(FireboltDataType.Struct));
        Assert.False(columnType.Nullable);
        Assert.Null(columnType.Precision);
        Assert.Null(columnType.Scale);
        if (columnType is not StructType structType)
        {
            Assert.Fail("StructType expected");
            return;
        }
        Assert.That(structType.Fields.Count, Is.EqualTo(2));
        Assert.That(structType.Fields["a"].Type, Is.EqualTo(FireboltDataType.Int));
        Assert.False(structType.Fields["a"].Nullable);
        Assert.Null(structType.Fields["a"].Precision);
        Assert.Null(structType.Fields["a"].Scale);
        Assert.That(structType.Fields["b"].Type, Is.EqualTo(FireboltDataType.String));
        Assert.False(structType.Fields["b"].Nullable);
        Assert.Null(structType.Fields["b"].Precision);
        Assert.Null(structType.Fields["b"].Scale);
    }

    [Test]
    public void CreateStructTypeWithSpacesFieldName()
    {
        String type = "struct(a int, `b c` text)";
        ColumnType columnType = ColumnType.Of(type);
        Assert.That(columnType.Type, Is.EqualTo(FireboltDataType.Struct));
        Assert.False(columnType.Nullable);
        Assert.Null(columnType.Precision);
        Assert.Null(columnType.Scale);
        if (columnType is not StructType structType)
        {
            Assert.Fail("StructType expected");
            return;
        }
        Assert.That(structType.Fields.Count, Is.EqualTo(2));
        Assert.That(structType.Fields["a"].Type, Is.EqualTo(FireboltDataType.Int));
        Assert.False(structType.Fields["a"].Nullable);
        Assert.Null(structType.Fields["a"].Precision);
        Assert.Null(structType.Fields["a"].Scale);
        Assert.That(structType.Fields["b c"].Type, Is.EqualTo(FireboltDataType.String));
        Assert.False(structType.Fields["b c"].Nullable);
        Assert.Null(structType.Fields["b c"].Precision);
        Assert.Null(structType.Fields["b c"].Scale);
    }

    [Test]
    public void CreateNestedStructTest()
    {
        String type = "struct(a int, b struct(c array(int), d text))";
        ColumnType columnType = ColumnType.Of(type);
        Assert.That(columnType.Type, Is.EqualTo(FireboltDataType.Struct));
        Assert.False(columnType.Nullable);
        Assert.Null(columnType.Precision);
        Assert.Null(columnType.Scale);
        if (columnType is not StructType structType)
        {
            Assert.Fail("StructType expected");
            return;
        }

        Assert.That(structType.Fields.Count, Is.EqualTo(2));
        Assert.That(structType.Fields["a"].Type, Is.EqualTo(FireboltDataType.Int));
        Assert.False(structType.Fields["a"].Nullable);
        Assert.Null(structType.Fields["a"].Precision);
        Assert.Null(structType.Fields["a"].Scale);
        Assert.That(structType.Fields["b"].Type, Is.EqualTo(FireboltDataType.Struct));
        if (structType.Fields["b"] is not StructType nestedStructType)
        {
            Assert.Fail("StructType expected");
            return;
        }

        Assert.That(nestedStructType.Fields.Count, Is.EqualTo(2));
        Assert.That(nestedStructType.Fields["c"].Type, Is.EqualTo(FireboltDataType.Array));
        if (nestedStructType.Fields["c"] is not ArrayType arrayType)
        {
            Assert.Fail("ArrayType expected");
            return;
        }

        Assert.That(arrayType.InnerType!.Type, Is.EqualTo(FireboltDataType.Int));
        Assert.False(arrayType.InnerType.Nullable);
        Assert.Null(arrayType.InnerType.Precision);
        Assert.Null(arrayType.InnerType.Scale);

        Assert.That(nestedStructType.Fields["d"].Type, Is.EqualTo(FireboltDataType.String));
        Assert.False(nestedStructType.Fields["d"].Nullable);
        Assert.Null(nestedStructType.Fields["d"].Precision);
        Assert.Null(nestedStructType.Fields["d"].Scale);
    }

    [Test]
    public void CreateNestedStructWithSpaces()
    {
        String type = "struct(s struct(`a b` int))";
        ColumnType columnType = ColumnType.Of(type);
        Assert.That(columnType.Type, Is.EqualTo(FireboltDataType.Struct));
        Assert.False(columnType.Nullable);
        Assert.Null(columnType.Precision);
        Assert.Null(columnType.Scale);
        if (columnType is not StructType structType)
        {
            Assert.Fail("StructType expected");
            return;
        }
        Assert.That(structType.Fields.Count, Is.EqualTo(1));
        Assert.That(structType.Fields["s"].Type, Is.EqualTo(FireboltDataType.Struct));
        if (structType.Fields["s"] is not StructType nestedStructType)
        {
            Assert.Fail("StructType expected");
            return;
        }
        Assert.That(nestedStructType.Fields.Count, Is.EqualTo(1));
        Assert.That(nestedStructType.Fields["a b"].Type, Is.EqualTo(FireboltDataType.Int));
    }
}