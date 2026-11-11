using Newtonsoft.Json;

namespace FireboltDotNetSdk.Tests.Helpers
{
    internal static class LargeParameterSet
    {
        // The cloud gateway rejects URLs over ~16 KB with 414, so ~10 KB of URL-escaped query_parameters fits and ~20 KB does not.
        // Escaping inflates the JSON roughly 1.6x, so the count is sized on the escaped length.
        public const int TenKbParameterCount = 135;
        public const int TwentyKbParameterCount = 425;
        // Matches the customer report.
        public const int CustomerParameterCount = 7350;

        public static string Value(int i) => $"value-{i:D5}-abcdefghij";

        public static string QueryParametersJson(int count)
        {
            return JsonConvert.SerializeObject(Enumerable.Range(1, count).Select(i => new { name = $"${i}", value = Value(i) }));
        }
    }
}
