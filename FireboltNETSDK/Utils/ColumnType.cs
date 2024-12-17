using System.Text.RegularExpressions;
using FireboltDoNetSdk.Utils;

namespace FireboltDotNetSdk.Utils;

public class ColumnType
{
    private static string NULL_TYPE = "NULL";
    public readonly int? Scale;
    public readonly int? Precision;
    public readonly FireboltDataType Type;
    public readonly bool Nullable;

    public ColumnType(int? precision, int? scale, FireboltDataType type, bool nullable)
    {
        Precision = precision;
        Scale = scale;
        Type = type;
        Nullable = nullable;
    }

    public static ColumnType Of(string fullColumnType)
    {
        ColumnType? innerDataType = null;
        Tuple<int?, int?>? scaleAndPrecisionPair = null;
        ColumnTypeWrapper columnTypeWrapper = ColumnTypeWrapper.Of(fullColumnType);
        string typeWithoutNullKeyword = columnTypeWrapper.TypeWithoutNullKeyword;
        int typeEndIndex = GetTypeEndPosition(typeWithoutNullKeyword);
        FireboltDataType dataType = GetFireboltDataTypeFromColumnType(typeWithoutNullKeyword, typeEndIndex);
        if (dataType.Equals(FireboltDataType.Array))
        {
            innerDataType = GetInnerTypes(typeWithoutNullKeyword);
            return new ArrayType(innerDataType, columnTypeWrapper.HasNullableKeyword);
        }
        if (dataType.Equals(FireboltDataType.Struct))
        {
            return new StructType(new Dictionary<string, ColumnType>(), columnTypeWrapper.HasNullableKeyword);
        }


        if (dataType.Equals(FireboltDataType.Decimal) &&
            (!ReachedEndOfTypeName(typeEndIndex, typeWithoutNullKeyword) || typeWithoutNullKeyword.Substring(typeEndIndex).StartsWith("(")))
        {
            String[] arguments = SplitArguments(typeWithoutNullKeyword, typeEndIndex);
            scaleAndPrecisionPair = GetScaleAndPrecision(arguments);
        }
        return new ColumnType(scaleAndPrecisionPair?.Item1, scaleAndPrecisionPair?.Item2, dataType, columnTypeWrapper.HasNullableKeyword || dataType == FireboltDataType.Null);
    }


    private static FireboltDataType GetFireboltDataTypeFromColumnType(string typeWithoutNullKeyword, int typeEndIndex)
    {

        List<string> nestedTypes = new List<string> { "array", "decimal", "numeric", "struct" };

        string columnType = nestedTypes.FirstOrDefault(
            type => typeWithoutNullKeyword.StartsWith(type),
            typeWithoutNullKeyword.Substring(0, typeEndIndex));
        return TypesConverter.MapColumnTypeToFireboltDataType(columnType);
    }

    private static Tuple<int?, int?> GetScaleAndPrecision(string[] arguments)
    {
        int? scale = null;
        int? precision = null;
        if (arguments.Length == 2)
        {
            precision = int.Parse(arguments[0]);
            scale = int.Parse(arguments[1]);
        }

        return new Tuple<int?, int?>(precision, scale);
    }

    private static ColumnType GetInnerTypes(string columnType)
    {
        return Of(GetArrayType(columnType).Trim());
    }

    private static string GetArrayType(string columnType)
    {
        Regex rgx = new Regex("(?i)ARRAY\\((?-i)");
        String types = rgx.Replace(columnType, "", 1); //Remove first ARRAY(
        return types.Remove(types.Length - 1, 1); // remove last )
    }

    private static int GetTypeEndPosition(string type)
    {
        int typeNameEndIndex = !type.Contains("(") ? type.IndexOf(")") : type.IndexOf("(");
        return typeNameEndIndex < 0 ? type.Length : typeNameEndIndex;
    }

    private static String[] SplitArguments(string args, int index)
    {
        int startIndex = args.IndexOf("(", index) + 1;
        int endIndex = args.IndexOf(")", index);
        return args.Substring(startIndex, endIndex - startIndex).Split(",");
    }

    private static bool ReachedEndOfTypeName(int typeNameEndIndex, string type)
    {
        return typeNameEndIndex == type.Length || type.IndexOf("(", typeNameEndIndex) < 0
                                               || type.IndexOf(")", typeNameEndIndex) < 0;
    }

    private class ColumnTypeWrapper
    {
        internal string Type { get; }
        internal string TypeWithoutNullKeyword { get; }
        internal bool HasNullableKeyword { get; }

        private ColumnTypeWrapper(string type, string typeWithoutNullKeyword, bool hasNullableKeyword)
        {
            Type = type;
            TypeWithoutNullKeyword = typeWithoutNullKeyword;
            HasNullableKeyword = hasNullableKeyword;
        }

        internal static ColumnTypeWrapper Of(string type)
        {
            bool containsNullableKeyword = false;
            string typeInUpperCase = type.ToUpper();
            string typeWithoutNullableKeyword;
            if (!typeInUpperCase.Equals(NULL_TYPE) && typeInUpperCase.EndsWith(NULL_TYPE))
            {
                containsNullableKeyword = true;
                typeWithoutNullableKeyword = type.Substring(0, typeInUpperCase.LastIndexOf(NULL_TYPE)).Trim();
            }
            else
            {
                typeWithoutNullableKeyword = type;
            }

            return new ColumnTypeWrapper(type, typeWithoutNullableKeyword, containsNullableKeyword);
        }
    }
}

public class ArrayType : ColumnType
{
    public ColumnType InnerType;

    public ArrayType(ColumnType innerType, bool nullable) : base(null, null, FireboltDataType.Array, nullable)
    {
        InnerType = innerType;
    }
}

public class StructType : ColumnType
{
    public readonly Dictionary<string, ColumnType> Fields;

    public StructType(Dictionary<string, ColumnType> fields, bool nullable) : base(null, null, FireboltDataType.Struct, nullable)
    {
        Fields = fields;
    }
}