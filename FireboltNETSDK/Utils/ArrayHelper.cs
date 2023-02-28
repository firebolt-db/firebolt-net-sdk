using System.Text;
using System.Text.RegularExpressions;
using FireboltDoNetSdk.Utils;
using FireboltDotNetSdk.Exception;

namespace FireboltDotNetSdk.Utils;

public class ArrayHelper
{
    public static Array TransformToSqlArray(string value, ColumnType columnType)
    {
        var dimensions = 0;
        value = RemoveNewlinesAndSpaces(value);
        for (int i = 0; i < value.Length; i++)
        {
            if (value[i] == '[')
                dimensions++;
            else
                break;
        }

        value = value.Substring(dimensions, value.Length - 2 * dimensions);
        return CreateArray(value, dimensions, columnType);
    }

    private static Array CreateArray(string arrayContent, int dimension, ColumnType columnType)
    {
        if (dimension == 1)
        {
            return ExtractArrayFromOneDimensionalArray(arrayContent, columnType);
        }

        return ExtractArrayFromMultiDimensionalArray(arrayContent, dimension, columnType);
    }

    private static Array ExtractArrayFromMultiDimensionalArray(string str, int dimension, ColumnType columnType)
    {
        string[] s = str.Split(GetArraySeparator(dimension));
        List<Array> list = new List<Array>();
        for (int x = 0; x < s.Length; x++)
            list.Add(CreateArray(s[x], dimension - 1, columnType));
        return list.ToArray();
    }

    private static Array ExtractArrayFromOneDimensionalArray(string arrayContent, ColumnType columnType)
    {
        var elements = SplitArrayContent(arrayContent)
            .Where(x => !string.IsNullOrEmpty(x)).Select(x => RemoveQuotesAndTransformNull(x)).ToList();
        Array currentArray = new dynamic[elements.Count];
        for (int i = 0; i < elements.Count; i++)
        {
            var innerType = columnType.GetArrayBaseColumnType();
            if (innerType == null)
            {
                throw new FireboltException("Unable to retrieve a Firebolt type of an array");
            }
            currentArray.SetValue(TypesConverter.ConvertToCSharpVal(elements[i], innerType), i);

        }
        return currentArray;
    }

    private static string GetArraySeparator(int dimension)
    {
        StringBuilder stringBuilder = new StringBuilder(",");
        for (int x = 1; x < dimension; x++)
        {
            stringBuilder.Insert(0, ']');
            stringBuilder.Append('[');
        }

        return stringBuilder.ToString();
    }

    private static string? RemoveQuotesAndTransformNull(string s)
    {
        if (string.Equals(s, "NULL", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }
        return Regex.Replace(s, "\'", string.Empty);
    }

    public static string RemoveNewlinesAndSpaces(string input)
    {
        StringBuilder result = new StringBuilder();
        bool inSingleQuotes = false;
        foreach (char c in input)
        {
            if (c == '\'')
            {
                inSingleQuotes = !inSingleQuotes;
            }
            if (!inSingleQuotes && (c == '\n' || c == ' '))
            {
                continue;
            }
            result.Append(c);
        }
        return result.ToString();
    }

    private static List<string> SplitArrayContent(string arrayContent)
    {
        int index = -1;
        int subStringStart = 0;
        bool isCurrentSubstringBetweenQuotes = false;
        List<string> elements = new List<string>();
        while (index < arrayContent.Length - 1)
        {
            index++;
            char currentChar = arrayContent[index];
            if (currentChar == '\'')
            {
                isCurrentSubstringBetweenQuotes = !isCurrentSubstringBetweenQuotes;
            }

            if (currentChar == ',' && !isCurrentSubstringBetweenQuotes && subStringStart != index)
            {
                elements.Add((arrayContent.Substring(subStringStart, index - subStringStart)));
                subStringStart = index + 1;
            }
        }

        elements.Add(arrayContent.Substring(subStringStart));
        return elements;
    }
}