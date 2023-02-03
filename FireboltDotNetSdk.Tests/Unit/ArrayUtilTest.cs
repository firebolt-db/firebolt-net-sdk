using FireboltDotNetSdk.Utils;

namespace FireboltDotNetSdk.Tests;

public class ArrayUtilTest
{
    [Test]
    public void CreateArrayOfIntegers()
    {
        String type = "array(int)";
        String value = "[1,2,3,4]";
        var arr = ArrayHelper.TransformToSqlArray(value, ColumnType.Of(type));
        Assert.That(arr, Is.EqualTo(new[] { 1, 2, 3, 4 }));
    }

    [Test]
    public void CreateEmptyArray()
    {
        String type = "array(int)";
        String value = "[]";
        var arr = ArrayHelper.TransformToSqlArray(value, ColumnType.Of(type));
        Assert.IsEmpty(arr);
    }

    [Test]
    public void CreateArrayOfIntegersWithNull()
    {
        String type = "array(int NULL)";
        String value = "[1,2,NULL,4]";
        var arr = ArrayHelper.TransformToSqlArray(value, ColumnType.Of(type));
        Assert.That(arr, Is.EqualTo(new Nullable<int>[] { 1, 2, null, 4 }));
    }

    [Test]
    public void CreateArrayOfStringsWithNull()
    {
        String type = "array(string NULL)";
        String value = "['1','2',NULL,'3']";
        var arr = ArrayHelper.TransformToSqlArray(value, ColumnType.Of(type));
        Assert.That(arr, Is.EqualTo(new[] { "1", "2", null, "3" }));
    }

    [Test]
    public void CreateTwoDimensionalArrayOfIntegers()
    {
        String type = "array(array(int))";
        String value = "[[1,2,3,4]]";
        int[][] jaggedArray = new int[1][];
        jaggedArray[0] = new[] { 1, 2, 3, 4 };
        var arr = ArrayHelper.TransformToSqlArray(value, ColumnType.Of(type));
        Assert.That(arr, Is.EqualTo(jaggedArray));
    }

    [Test]
    public void CreateTwoDimensionalArrayOfNullableDecimal()
    {
        String type = "array(array(decimal(28,30) null))";
        String value =
            "[[1231232.12345999999045705484425870653699999999999999999,2.000000000000000000000000000001,3.000000000000000000000000008,4.00000000000000000000000000009,4.99999999999999999999999999999,4.200000000000000000000000000000]]";
        Decimal[][] jaggedArray = new Decimal[1][];
        jaggedArray[0] = new[]
        {
            1231232.123459999990457054844258706537m, 2m, 3.000000000000000000000000008m,
            4.0000000000000000000000000001m, 5m, 4.2m
        };
        var arr = ArrayHelper.TransformToSqlArray(value, ColumnType.Of(type));
        Assert.That(arr, Is.EqualTo(jaggedArray));
    }

    [Test]
    public void CreateArrayOfEmptyArrays()
    {
        String type = "array(array(int))";
        String value = "[[],[]]";
        var expected = new[] { new int[0], new int[0] };
        var arr = ArrayHelper.TransformToSqlArray(value, ColumnType.Of(type));
        Assert.That(arr, Is.EqualTo(expected));
    }

    [Test]
    public void CreateThreeDimensionalArrayOfIntegers()
    {
        String type = "array(array(array(int null)))";
        String value = "[[[1,2,null,4]],[[5,6,null,7]]]";
        int?[][][] jagged3d = { new[] { new int?[] { 1, 2, null, 4 } }, new[] { new int?[] { 5, 6, null, 7 } } };
        var arr = ArrayHelper.TransformToSqlArray(value, ColumnType.Of(type));
        Assert.That(arr, Is.EqualTo(jagged3d));
    }

    [Test]
    public void CreateFourDimensionalArrayOfIntegers()
    {
        String type = "array(array(array(array(int null))))";
        String value = "[[[[1,2,3,4]]]]";
        int[][][][] jagged4d = { new[] { new[] { new[] { 1, 2, 3, 4 } } } };
        var arr = ArrayHelper.TransformToSqlArray(value, ColumnType.Of(type));
        Assert.That(arr, Is.EqualTo(jagged4d));
    }

    [Test]
    public void CreateArrayOfIntegersWithEscapeCharacters()
    {
        String type = "array(integer)";
        String value = "[1,\n  2,\n  3,NULL\n,  4\n]";
        var arr = ArrayHelper.TransformToSqlArray(value, ColumnType.Of(type));
        Assert.That(arr, Is.EqualTo(new int?[] { 1, 2, 3, null, 4 }));
    }

    [Test]
    public void CreateTwoDimensionalArrayWithoutEscapeCharacters()
    {
        String type = "array(array(integer))";
        String value = "[\n    [\n      1,\n      2,\n      3,\n      4\n    ]\n  ]";
        int[][] jaggedArray = new int[1][];
        jaggedArray[0] = new[] { 1, 2, 3, 4 };
        var arr = ArrayHelper.TransformToSqlArray(value, ColumnType.Of(type));
    }
    
    [Test]
    public void CreateArrayOfStringsWithEscapeCharacters()
    {
        String type = "array(string NULL)";
        String value = "['1','\n      2',NULL,'3']";
        var arr = ArrayHelper.TransformToSqlArray(value, ColumnType.Of(type));
        Assert.That(arr, Is.EqualTo(new[] { "1", "\n      2", null, "3" }));
    }

}