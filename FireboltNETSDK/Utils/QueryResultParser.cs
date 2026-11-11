using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace FireboltDotNetSdk.Utils
{
    internal static class QueryResultParser
    {
        // Created with JsonSerializer.Create, not CreateDefault, so the host's JsonConvert.DefaultSettings cannot change
        // how results are read. Replace keeps the last value of a repeated key, as JToken.Parse did, as long as that value
        // is non-null and of the right shape; the default would append to the list already read.
        // Cells are converted by column type later, so strings stay as sent: date detection turned TEXT values that
        // look like ISO dates into DateTime, which then came back culture-formatted.
        private static readonly JsonSerializer serializer = JsonSerializer.Create(new JsonSerializerSettings
        {
            ObjectCreationHandling = ObjectCreationHandling.Replace,
            CheckAdditionalContent = true,
            DateParseHandling = DateParseHandling.None,
        });

        /// <summary>
        /// Deserializes JSON from the server with the same settings as query results.
        /// </summary>
        internal static T? Deserialize<T>(string json)
        {
            return (T?)Deserialize(json, typeof(T));
        }

        internal static object? Deserialize(string json, System.Type type)
        {
            using var reader = new JsonTextReader(new StringReader(json));
            return serializer.Deserialize(reader, type);
        }

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
                result = Deserialize<QueryResult>(response);
            }
            catch (JsonReaderException)
            {
                throw;
            }
            // Which exception malformed JSON (e.g. a truncated body) surfaces as is up to Json.NET, so classify any
            // other failure by checking the syntax, on this failure path only. Re-parsing a body that ran out of memory
            // would only make it worse.
            catch (System.Exception e) when (e is not OutOfMemoryException && TryGetSyntaxError(response, out var syntaxError))
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
