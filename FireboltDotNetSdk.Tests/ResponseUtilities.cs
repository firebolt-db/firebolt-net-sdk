using FireboltDotNetSdk.Client;

namespace FireboltDotNetSdk.Tests;

public static class ResponseUtilities
{
    public static NewMeta getFirstRow(string response)
    {
        var enumerator = FireboltCommand.FormDataForResponse(response).GetEnumerator();
        enumerator.MoveNext();
        return enumerator.Current;
    }
}