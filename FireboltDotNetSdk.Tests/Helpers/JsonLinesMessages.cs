using Newtonsoft.Json;

namespace FireboltDotNetSdk.Tests.Helpers
{
    /// <summary>
    /// Builds the messages of Firebolt's JSONLines_Compact output format, one line each without the newline.
    /// </summary>
    internal static class JsonLinesMessages
    {
        public const string Finish = "{\"message_type\":\"FINISH_SUCCESSFULLY\"}";

        public static string Start(params (string Name, string Type)[] columns)
        {
            return JsonConvert.SerializeObject(new
            {
                message_type = "START",
                result_columns = columns.Select(c => new { name = c.Name, type = c.Type })
            });
        }

        public static string Data(IEnumerable<object?[]> rows)
        {
            return JsonConvert.SerializeObject(new { message_type = "DATA", data = rows });
        }

        public static string FinishWithErrors(params string[] descriptions)
        {
            return JsonConvert.SerializeObject(new
            {
                message_type = "FINISH_WITH_ERRORS",
                errors = descriptions.Select(d => new { description = d })
            });
        }
    }
}
