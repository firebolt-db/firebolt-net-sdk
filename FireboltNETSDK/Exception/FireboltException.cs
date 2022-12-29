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

namespace FireboltDotNetSdk.Exception;

public class FireboltException : System.Exception
{
    public FireboltException(string message, int statusCode, string response,
        IReadOnlyDictionary<string, IEnumerable<string>> headers, System.Exception? innerException)
        : base(
            message + "\n\nStatus: " + statusCode + "\nResponse: \n" + (response == null
                ? "(null)"
                : response.Substring(0, response.Length >= 512 ? 512 : response.Length)), innerException)
    {
        StatusCode = statusCode;
        Response = response;
        Headers = headers;
    }

    public FireboltException(string message) : base(message)
    {
    }

    private int StatusCode { get; }

    private string Response { get; }

    private IReadOnlyDictionary<string, IEnumerable<string>> Headers { get; }

    public override string ToString()
    {
        return $"HTTP Response: \n\n{Response}\n\n{base.ToString()}";
    }
}

public class FireboltException<TResult> : FireboltException
{
    public FireboltException(string message, int statusCode, string response,
        IReadOnlyDictionary<string, IEnumerable<string>> headers, TResult result, System.Exception? innerException)
        : base(message, statusCode, response, headers, innerException)
    {
        Result = result;
    }

    private TResult Result { get; }
}

public abstract class Error
{
    public int? StatusCode { get; set; }
}