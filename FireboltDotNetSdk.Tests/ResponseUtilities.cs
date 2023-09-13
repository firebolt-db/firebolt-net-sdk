using FireboltDoNetSdk.Utils;

namespace FireboltDotNetSdk.Tests;

public class ResponseUtilities
{
    public static NewMeta getFirstRow(string response)
    {
        var enumerator = TypesConverter.ParseJsonResponse(response).GetEnumerator();
        enumerator.MoveNext();
        return enumerator.Current;
    }
}