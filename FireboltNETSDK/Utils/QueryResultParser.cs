using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace FireboltDotNetSdk.Utils
{
    internal static class QueryResultParser
    {
        // Created with JsonSerializer.Create, not CreateDefault, so the host's JsonConvert.DefaultSettings cannot change
        // how results are read. Replace keeps the last value of a repeated key, as JToken.Parse did; the default would
        // append to the list already read.
        private static readonly JsonSerializerSettings settings = new()
        {
            ObjectCreationHandling = ObjectCreationHandling.Replace,
            CheckAdditionalContent = true,
        };

        /// <summary>
        /// Deserializes a JSON query response in a single pass.
        /// </summary>
        /// <returns>The result, or null for a JSON null body.</returns>
        /// <exception cref="JsonReaderException">The body is not well-formed JSON.</exception>
        /// <exception cref="JsonSerializationException">The body is well-formed JSON of the wrong shape.</exception>
        internal static QueryResult? Parse(string response)
        {
            QueryResult? result;
            try
            {
                using var reader = new JsonTextReader(new StringReader(response));
                result = JsonSerializer.Create(settings).Deserialize<QueryResult>(reader);
            }
            catch (JsonReaderException)
            {
                throw;
            }
            // Which exception malformed JSON (e.g. a truncated body) surfaces as is up to Json.NET, so classify any
            // other failure by checking the syntax, on this failure path only.
            catch (System.Exception) when (TryGetSyntaxError(response, out var syntaxError))
            {
                throw syntaxError!;
            }
            // The serializer also returns null for a body without any JSON value, e.g. blank or only a comment.
            if (result == null)
            {
                if (TryGetSyntaxError(response, out var emptyBodyError))
                {
                    throw emptyBodyError!;
                }
                if (!ContainsJsonValue(response))
                {
                    throw new JsonReaderException("Response body contains no JSON value.");
                }
            }
            return result;
        }

        private static bool ContainsJsonValue(string json)
        {
            using var reader = new JsonTextReader(new StringReader(json));
            while (reader.Read())
            {
                if (reader.TokenType != JsonToken.Comment)
                {
                    return true;
                }
            }
            return false;
        }

        private static bool TryGetSyntaxError(string json, out JsonReaderException? syntaxError)
        {
            try
            {
                JToken.Parse(json);
                syntaxError = null;
                return false;
            }
            catch (JsonReaderException e)
            {
                syntaxError = e;
                return true;
            }
        }
    }
}
