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

namespace FireboltDotNetSdk.Exception
{
    public class FireboltException : System.Exception
    {
        private int StatusCode { get; }

        internal string? Response { get; }

        private IReadOnlyDictionary<string, IEnumerable<string>>? Headers { get; }

        public FireboltException(string message, int statusCode, string? response) : this(message, statusCode, response,
            null, null)
        { }
        
        public FireboltException(string message, int statusCode, string? response, IReadOnlyDictionary<string, IEnumerable<string>>? headers, System.Exception? innerException)
            : base(FormatServerError(message, statusCode, response), innerException)
        {
            StatusCode = statusCode;
            Response = response;
            Headers = headers;
        }

        public FireboltException(string message) : base(message)
        {
        }

        public override string ToString()
        {
            return $"HTTP Response: {Environment.NewLine}{Response}{Environment.NewLine}{base.ToString()}";
        }

        private static string FormatServerError(string error, int statusCode, string? serverError)
        {
            var errorMessage = $"{error}{Environment.NewLine}Status: {statusCode}";

            if (!string.IsNullOrWhiteSpace(serverError))
            {
                return $"{errorMessage}\nResponse:\n{serverError.Substring(0, serverError.Length >= 512 ? 512 : serverError.Length)}";
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
