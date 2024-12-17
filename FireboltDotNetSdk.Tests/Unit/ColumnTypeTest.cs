using FireboltDoNetSdk.Utils;
using FireboltDotNetSdk.Utils;

namespace FireboltDotNetSdk.Tests;

public class ColumnTypeTest
{
    [Test]
    public void CreateColumnTypeWithArrayTypeTest()
    {
        String type = "array(integer NULL)";
        ColumnType columnType = ColumnType.Of(type);
        if (columnType is ArrayType arrayType)
        {
            Assert.That(arrayType.Type, Is.EqualTo(FireboltDataType.Array));
            Assert.False(arrayType.Nullable);
            Assert.Null(arrayType.Precision);
            Assert.Null(arrayType.Scale);
            Assert.NotNull(arrayType.InnerType);
            Assert.True(arrayType.InnerType!.Nullable);
            Assert.That(arrayType.InnerType.Type, Is.EqualTo(FireboltDataType.Int));
        }
        else
        {
            Assert.Fail("ArrayType expected");
        }
    }

    [Test]
    public void CreateColumnTypeWithNullableArrayTypeTest()
    {
        String type = "array(integer NULL) NULL";
        ColumnType columnType = ColumnType.Of(type);
        if (columnType is ArrayType arrayType)
        {
            Assert.That(arrayType.Type, Is.EqualTo(FireboltDataType.Array));
            Assert.True(arrayType.Nullable);
            Assert.Null(arrayType.Precision);
            Assert.Null(arrayType.Scale);
            Assert.NotNull(arrayType.InnerType);
            Assert.True(arrayType.InnerType!.Nullable);
            Assert.That(arrayType.InnerType.Type, Is.EqualTo(FireboltDataType.Int));
        }
        else
        {
            Assert.Fail("ArrayType expected");
        }
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
        if (columnType is ArrayType arrayType)
        {
            Assert.That(arrayType.Type, Is.EqualTo(FireboltDataType.Array));
            Assert.False(arrayType.Nullable);
            Assert.Null(arrayType.Precision);
            Assert.Null(arrayType.Scale);
            Assert.NotNull(arrayType.InnerType);
            Assert.True(arrayType.InnerType!.Nullable);
            Assert.That(arrayType.InnerType.Type, Is.EqualTo(FireboltDataType.Array));
            if (arrayType.InnerType is ArrayType innerArrayType)
            {
                Assert.NotNull(innerArrayType.InnerType);
                Assert.True(innerArrayType.InnerType!.Nullable);
                Assert.That(innerArrayType.InnerType.Type, Is.EqualTo(FireboltDataType.Decimal));
                Assert.That(innerArrayType.InnerType.Precision, Is.EqualTo(5));
                Assert.That(innerArrayType.InnerType.Scale, Is.EqualTo(2));
            }
            else
            {
                Assert.Fail("ArrayType expected");
            }
        }
        else
        {
            Assert.Fail("ArrayType expected");
        }
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

    /*[Test]
    public void CreateStructTypeWithProvidedColumnTypeNamesTest()
    {
        String columnTypeName = "struct(a int, s struct(a array(int), b text null))";
        
        ColumnType columnType = ColumnType.Of(columnTypeName);
        if (columnType is StructType structType)
        Assert.That(columnType.Type, Is.EqualTo(expectedType));
        Assert.That(columnType.Nullable, Is.EqualTo(expectedIsNullable));
        Assert.Null(columnType.Precision);
        Assert.Null(columnType.Scale);
    }*/



}