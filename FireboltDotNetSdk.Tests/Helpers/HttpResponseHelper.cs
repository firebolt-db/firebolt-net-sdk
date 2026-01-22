using System.Net;
using System.Text;
using static Newtonsoft.Json.JsonConvert;

namespace FireboltDotNetSdk.Tests.Helpers
{
    /// <summary>
    /// Helper class for creating HTTP response messages in tests
    /// </summary>
    public static class HttpResponseHelper
    {
        /// <summary>
        /// Creates an HTTP response message with the specified object, status code, and optional headers
        /// </summary>
        /// <param name="responseObject">The object to serialize (or string content)</param>
        /// <param name="httpStatusCode">The HTTP status code</param>
        /// <param name="headers">Optional dictionary of headers to add to the response</param>
        /// <returns>An HttpResponseMessage with the specified content and headers</returns>
        public static HttpResponseMessage GetResponseMessage(object responseObject, HttpStatusCode httpStatusCode,
            Dictionary<string, string>? headers = null)
        {
            var response = GetResponseMessage(httpStatusCode);
            if (responseObject is string responseAsString)
            {
                response.Content = new StringContent(responseAsString);
            }
            else
            {
                response.Content =
                    new StringContent(SerializeObject(responseObject), Encoding.UTF8, "application/json");
            }

            if (headers == null) return response;
            foreach (var header in headers)
            {
                response.Headers.Add(header.Key, header.Value);
            }

            return response;
        }

        /// <summary>
        /// Creates an HTTP response message with the specified status code
        /// </summary>
        /// <param name="httpStatusCode">The HTTP status code</param>
        /// <returns>An HttpResponseMessage with the specified status code</returns>
        public static HttpResponseMessage GetResponseMessage(HttpStatusCode httpStatusCode)
        {
            return new HttpResponseMessage() { StatusCode = httpStatusCode };
        }
    }
}
