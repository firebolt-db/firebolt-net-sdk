#region License Apache 2.0
/* Copyright 2022 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#endregion

using System.Net;

namespace FireboltDotNetSdk.Exception
{
    public class FireboltException : System.Exception
    {

        private const int MaxDisplayableServerErrorLength = 512;
        internal Nullable<HttpStatusCode> StatusCode { get; set; }

        internal string? Response { get; }

        private IReadOnlyDictionary<string, IEnumerable<string>>? Headers { get; }



        public FireboltException(HttpStatusCode statusCode, string? response) : this(getErrorMessageFromStatusCode(statusCode), statusCode, response,
            null, null)
        { }

        public FireboltException(string message, System.Exception? innerException) : base(message, innerException)
        { }

        public FireboltException(string message, HttpStatusCode statusCode, string? response, IReadOnlyDictionary<string, IEnumerable<string>>? headers, System.Exception? innerException)
            : base(FormatServerError(message, statusCode, response), innerException)
        {
            StatusCode = statusCode;
            Response = response;
            Headers = headers;
        }

        public FireboltException(string message) : base(message)
        {
        }

        private static string getErrorMessageFromStatusCode(HttpStatusCode statusCode)
        {
            string exceptionMessage;
            switch (statusCode)
            {
                case HttpStatusCode.Unauthorized:
                    exceptionMessage = "The operation is unauthorized";
                    break;
                case HttpStatusCode.Forbidden:
                    exceptionMessage = "The operation is forbidden";
                    break;
                default:
                    exceptionMessage = "Received an unexpected status code from the server";
                    break;
            }

            return exceptionMessage;
        }

        public override string ToString()
        {
            if (StatusCode == null || String.IsNullOrEmpty(Response))
            {
                return base.ToString();
            }
            return $"HTTP Response: {Response}{Environment.NewLine}{base.ToString()}";
        }

        private static string FormatServerError(string error, HttpStatusCode statusCode, string? serverError)
        {
            var errorMessage = $"{error}{Environment.NewLine}Status: {(int)statusCode}";

            if (!string.IsNullOrWhiteSpace(serverError))
            {
                return $"{errorMessage}\nResponse:\n{serverError.Substring(0, serverError.Length >= MaxDisplayableServerErrorLength ? MaxDisplayableServerErrorLength : serverError.Length)}";
            }

            return errorMessage;
        }
    }

    public class ResponseError
    {
        public string? error { get; set; }
        public int? code { get; set; }
        public string? message { get; set; }

    }

}
