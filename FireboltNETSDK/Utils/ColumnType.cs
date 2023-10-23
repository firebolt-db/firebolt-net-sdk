using System.Text.RegularExpressions;
using FireboltDoNetSdk.Utils;

namespace FireboltDotNetSdk.Utils;

public class ColumnType
{
    private static string NULL_TYPE = "NULL";
    public readonly int? Scale;
    public readonly int? Precision;
    public readonly FireboltDataType Type;
    public ColumnType? InnerType;
    public readonly bool Nullable;

    public ColumnType(int? precision, int? scale, FireboltDataType type, bool nullable, ColumnType? innerType)
    {
        Precision = precision;
        Scale = scale;
        Type = type;
        InnerType = innerType;
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
        }

        String[] arguments;
        if (dataType.Equals(FireboltDataType.Decimal) &&
            (!ReachedEndOfTypeName(typeEndIndex, typeWithoutNullKeyword) || typeWithoutNullKeyword.Substring(typeEndIndex).StartsWith("(")))
        {
            arguments = SplitArguments(typeWithoutNullKeyword, typeEndIndex);
            scaleAndPrecisionPair = GetScaleAndPrecision(arguments);
        }
        return new ColumnType(scaleAndPrecisionPair?.Item1, scaleAndPrecisionPair?.Item2, dataType, columnTypeWrapper.HasNullableKeyword || dataType == FireboltDataType.Null, innerDataType);
    }


    private static FireboltDataType GetFireboltDataTypeFromColumnType(string typeWithoutNullKeyword, int typeEndIndex)
    {
        string columnType;
        if (typeWithoutNullKeyword.StartsWith("array"))
        {
            columnType = "array";
        }
        else if (typeWithoutNullKeyword.StartsWith("decimal"))
        {
            columnType = "decimal";
        }
        else if (typeWithoutNullKeyword.StartsWith("numeric"))
        {
            columnType = "numeric";
        }
        else
        {
            columnType = typeWithoutNullKeyword.Substring(0, typeEndIndex);
        }
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

    public ColumnType? GetArrayBaseColumnType()
    {
        if (InnerType == null)
        {
            return null;
        }

        ColumnType currentInnerType = InnerType;
        while (currentInnerType.InnerType != null)
        {
            currentInnerType = currentInnerType.InnerType;
        }
        return currentInnerType;
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