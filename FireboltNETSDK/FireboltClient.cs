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

using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
using FireboltDotNetSdk.Exception;
using Newtonsoft.Json;
using static FireboltDotNetSdk.Client.FireRequest;
using static FireboltDotNetSdk.Client.FireResponse;

namespace FireboltDotNetSdk;

public class FireboltClient
{
    private static FireboltClient? _instance;
    private static readonly object Mutex = new();
    private readonly Lazy<JsonSerializerSettings> _settings;
    public HttpClient HttpClient { get; }
    
    private FireboltClient()
    {
        HttpClient = CreateClient();
        _settings = new Lazy<JsonSerializerSettings>(new JsonSerializerSettings());
    }
    
    /// <summary>
    ///     Returns a shared instance of the Firebolt client.
    /// </summary>
    public static FireboltClient GetInstance()
    {
        if (_instance == null)
            lock (Mutex)
            {
                _instance ??= new FireboltClient();
            }
        return _instance;
    }
    
    private HttpClient CreateClient()
    {
        var client = new HttpClient();

        // Disable timeouts
        client.Timeout = TimeSpan.FromMilliseconds(-1);

        var version = Assembly.GetEntryAssembly()?.GetName()?.Version?.ToString();
        client.DefaultRequestHeaders.Add("User-Agent", ".NETSDK/.NET6_" + version);
        return client;
    }

    private JsonSerializerSettings JsonSerializerSettings => _settings.Value;
    
    /// <summary>
    ///     Authenticates the user with Firebolt.
    /// </summary>
    /// <param name="loginRequest"></param>
    /// <param name="baseUrl"></param>
    /// <returns></returns>
    public Task<LoginResponse> Login(LoginRequest loginRequest, string baseUrl)
    {
        return AuthV1LoginAsync(loginRequest, CancellationToken.None, baseUrl);
    }

    /// <summary>
    ///     Returns engine URL by database name given.
    /// </summary>
    /// <param name="databaseName">Name of the database.</param>
    /// <param name="account"></param>
    /// <param name="baseUrl"></param>
    /// <param name="accessToken"></param>
    /// <returns>A successful response.</returns>
    public Task<GetEngineUrlByDatabaseNameResponse> GetEngineUrlByDatabaseName(string? databaseName,
        string? account, string baseUrl, string accessToken)
    {
        return CoreV1GetEngineUrlByDatabaseNameAsync(databaseName, account, baseUrl, accessToken, CancellationToken.None);
    }

    /// <summary>
    ///     Returns engine URL by engine name given.
    /// </summary>
    /// <param name="engine">Name of the engine.</param>
    /// <param name="account">Name of the account</param>
    /// <param name="baseUrl"></param>
    /// <param name="accessToken"></param>
    /// <returns>A successful response.</returns>
    public Task<GetEngineNameByEngineIdResponse> GetEngineUrlByEngineName(string engine, string? account,
        string baseUrl, string accessToken)
    {
        return CoreV1GetEngineUrlByEngineNameAsync(engine, account, baseUrl, accessToken, CancellationToken.None);
    }

    /// <summary>
    ///     Returns engine URL by engine id.
    /// </summary>
    /// <param name="engineId">Name of the database.</param>
    /// <param name="accountId"></param>
    /// <param name="baseUrl"></param>
    /// <param name="accessToken"></param>
    /// <returns>An Engine url response.</returns>
    public Task<GetEngineUrlByEngineNameResponse> GetEngineUrlByEngineId(string engineId, string accountId,
        string baseUrl, string accessToken)
    {
        return CoreV1GetEngineUrlByEngineIdAsync(engineId, accountId, CancellationToken.None, baseUrl, accessToken);
    }

    /// <summary>
    ///     Executes a SQL query
    /// </summary>
    /// <param name="engineEndpoint">Engine endpoint (URL)</param>
    /// <param name="databaseName">Database name</param>
    /// <param name="query">SQL query to execute</param>
    /// <param name="accessToken">an access token</param>
    /// <returns>A successful response.</returns>
    public Task<string?> ExecuteQuery(string? engineEndpoint, string databaseName, string query, string accessToken)
    {
        return ExecuteQueryAsync(engineEndpoint, databaseName, query,
            new HashSet<string>(), accessToken, CancellationToken.None);
    }

    /// <param name="engineEndpoint">Engine endpoint (URL)</param>
    /// <param name="databaseName">Database name</param>
    /// <param name="query">SQL query to execute</param>
    /// <param name="setParamList"></param>
    /// <param name="accessToken"></param>
    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Executes a SQL query
    /// </summary>
    /// <returns>A successful response.</returns>
    /// <exception cref="FireboltException">A server side error occurred.</exception>
    public async Task<string?> ExecuteQueryAsync(string? engineEndpoint, string databaseName, string query,
        HashSet<string> setParamList, string accessToken,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(engineEndpoint) || string.IsNullOrEmpty(databaseName) ||
            string.IsNullOrEmpty(query))
            throw new FireboltException(
                $"Something parameters are null or empty: engineEndpoint: {engineEndpoint}, databaseName: {databaseName} or query: {query}");

        var urlBuilder = new StringBuilder();
        var setParam = setParamList.Aggregate(string.Empty, (current, item) => current + "&" + item);
        urlBuilder.Append("https://").Append(engineEndpoint).Append("?database=").Append(databaseName)
            .Append(setParam).Append("&output_format=JSONCompact");

        using var request = new HttpRequestMessage();
        var content = new StringContent(query);
        content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json");
        request.Content = content;
        request.Method = new HttpMethod("POST");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("text/plain"));

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        AddAccessToken(request, accessToken);

        var response = await HttpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        
        try
        {
            var headers = response.Headers.ToDictionary(header => header.Key, header => header.Value);
            foreach (var item in response.Content.Headers)
                headers[item.Key] = item.Value;
            
            var objectResponse =
                await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            
            if (response.IsSuccessStatusCode)
            {
                if (objectResponse == null)
                    throw new FireboltException("Response was null which was not expected.", (int)response.StatusCode,
                        objectResponse ?? string.Empty, headers, null);

                return objectResponse;
            }
            else
            {
                throw new FireboltException("Response was null which was not expected with status: " + (int)response.StatusCode +
                                            ", body: " + objectResponse);
            }
        }
        finally
        {

                response.Dispose();
        }
    }
    
    private async Task<ObjectResponseResult<T?>> ReadObjectResponseAsync<T>(HttpResponseMessage? response,
        IReadOnlyDictionary<string, IEnumerable<string>> headers, bool readResponseAsString, CancellationToken cancellationToken)
    {
        if (response == null) return new ObjectResponseResult<T?>(default, string.Empty);

        if (readResponseAsString)
        {
            var responseText = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var typedBody = JsonConvert.DeserializeObject<T>(responseText, JsonSerializerSettings);
                return new ObjectResponseResult<T?>(typedBody, responseText);
            }
            catch (JsonException exception)
            {
                var message = "Could not deserialize the response body string as " + typeof(T).FullName + ".";
                throw new FireboltException(message, (int)response.StatusCode, responseText, headers, exception);
            }
        }

        try
        {
            await using var responseStream =
                await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            using var streamReader = new StreamReader(responseStream);
            using var jsonTextReader = new JsonTextReader(streamReader);
            var serializer = JsonSerializer.Create(JsonSerializerSettings);
            var typedBody = serializer.Deserialize<T>(jsonTextReader);
            return new ObjectResponseResult<T?>(typedBody, string.Empty);
        }
        catch (JsonException exception)
        {
            var message = "Could not deserialize the response body stream as " + typeof(T).FullName + ".";
            throw new FireboltException(message, (int)response.StatusCode, string.Empty, headers, exception);
        }
    }

    /// <param name="account"></param>
    /// <param name="baseUrl"></param>
    /// <param name="accessToken"></param>
    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Returns Account id by account name given.
    /// </summary>
    /// <returns>A successful response.</returns>
    /// <exception cref="FireboltException">A server side error occurred.</exception>
    public async Task<GetAccountIdByNameResponse> GetAccountIdByNameAsync(string account,
        string baseUrl, string? accessToken, CancellationToken cancellationToken)
    {
        if (account == null) throw new FireboltException("Account name is empty");

        //Get на /iam/v2/accounts:getIdByName
        var urlBuilder = new StringBuilder();
        urlBuilder.Append(baseUrl.TrimEnd('/')).Append("/iam/v2/accounts:getIdByName?");
        urlBuilder.Append(Uri.EscapeDataString("account_name") + "=")
            .Append(Uri.EscapeDataString(ConvertToString(account,
                CultureInfo.InvariantCulture)));
        using var request = new HttpRequestMessage();
        request.Method = new HttpMethod("GET");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        AddAccessToken(request, accessToken);

        var response = await HttpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);


        try
        {
            var headers = response.Headers.ToDictionary(h => h.Key, h => h.Value);
            foreach (var item in response.Content.Headers)
                headers[item.Key] = item.Value;

            if (response.IsSuccessStatusCode)
            {
                var objectResponse =
                    await ReadObjectResponseAsync<GetAccountIdByNameResponse>(response, headers, false,
                        cancellationToken).ConfigureAwait(false);

                if (objectResponse.Object == null)
                    throw new FireboltException("Response was null which was not expected.", (int)response.StatusCode,
                        objectResponse.Text, headers, null);

                return objectResponse.Object;
            }
            else
            {
                var objectResponse = ReadObjectResponseAsync<Error>(response, headers, false, cancellationToken)
                    .ConfigureAwait(false);
                throw new FireboltException("An unexpected error response");
            }
        }
        finally
        {
            response.Dispose();
        }
    }
    


    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Creates new user session
    /// </summary>
    /// <param name="body">Login credentials</param>
    /// <returns>A successful response.</returns>
    /// <exception cref="FireboltException">A server side error occurred.</exception>
    private async Task<LoginResponse> AuthV1LoginAsync(LoginRequest body, CancellationToken cancellationToken,
        string baseUrl)
    {
        if (body == null)
            throw new ArgumentNullException(nameof(body));

        var urlBuilder = new StringBuilder();
        urlBuilder.Append(baseUrl.TrimEnd('/')).Append("/auth/v1/login");

        using var request = new HttpRequestMessage();
        var content = new StringContent(JsonConvert.SerializeObject(body, _settings.Value));
        content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json");
        request.Content = content;
        request.Method = new HttpMethod("POST");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        var response = await HttpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);

        try
        {
            var headers = response.Headers.ToDictionary(h => h.Key, h => h.Value);
            foreach (var item in response.Content.Headers)
                headers[item.Key] = item.Value;
            
            if (response.IsSuccessStatusCode)
            {
                var objectResponse =
                    await ReadObjectResponseAsync<LoginResponse>(response, headers, false, cancellationToken)
                        .ConfigureAwait(false);
                if (objectResponse.Object == null)
                    throw new FireboltException("Response was null which was not expected.", (int)HttpStatusCode.OK,
                        objectResponse.Text, headers, null);

                return objectResponse.Object;
            }
            else
            {
                var objectResponse = await ReadObjectResponseAsync<Error>(response, headers, false, cancellationToken)
                    .ConfigureAwait(false);
                if (objectResponse.Object == null)
                    throw new FireboltException("Response was null which was not expected.", (int)response.StatusCode,
                        objectResponse.Text, headers, null);

                throw new FireboltException<Error>("An unexpected error response", (int)response.StatusCode, objectResponse.Text,
                    headers, objectResponse.Object, null);
            }
        }
        finally
        {
            response.Dispose();
        }
    }

    private static string ConvertToString(object? value, CultureInfo cultureInfo)
    {
        switch (value)
        {
            case Enum:
            {
                var name = Enum.GetName(value.GetType(), value);
                if (name != null)
                {
                    var field = value.GetType().GetTypeInfo().GetDeclaredField(name);
                    if (field != null)
                        if (field.GetCustomAttribute(typeof(EnumMemberAttribute)) is EnumMemberAttribute attribute)
                            return attribute.Value ?? name;

                    var converted = Convert.ToString(Convert.ChangeType(value,
                        Enum.GetUnderlyingType(value.GetType()), cultureInfo));
                    return converted ?? string.Empty;
                }

                break;
            }
            case bool b:
                return Convert.ToString(b, cultureInfo).ToLowerInvariant();
            case byte[] bytes:
                return Convert.ToBase64String(bytes);
            default:
            {
                if (value?.GetType().IsArray ?? false)
                {
                    IEnumerable<object?> array = ((Array)value).OfType<object>();
                    return string.Join(",", array.Select(o => ConvertToString(o, cultureInfo)));
                }

                break;
            }
        }

        var result = Convert.ToString(value, cultureInfo);
        return result ?? "";
    }

    private async Task<GetEngineUrlByEngineNameResponse> CoreV1GetEngineUrlByEngineIdAsync(string engineId,
        string accountId, CancellationToken cancellationToken, string baseUrl, string? accessToken)
    {
        if (engineId == null) throw new FireboltException("Engine name is incorrect or missing");

        var urlBuilder = new StringBuilder();
        urlBuilder.Append(baseUrl.TrimEnd('/'))
            .Append("/core/v1/accounts/" + accountId + "/engines/" + engineId);

        using var request = new HttpRequestMessage();
        request.Method = new HttpMethod("GET");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));
        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        AddAccessToken(request, accessToken);

        var response = await HttpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        var disposeResponse = true;
        try
        {
            var headers = response.Headers.ToDictionary(h => h.Key, h => h.Value);
            foreach (var item in response.Content.Headers)
                headers[item.Key] = item.Value;
            
            if (response.IsSuccessStatusCode)
            {
                var objectResponse =
                    await ReadObjectResponseAsync<GetEngineUrlByEngineNameResponse>(response, headers, false,
                        cancellationToken).ConfigureAwait(false);

                if (objectResponse.Object == null)
                    throw new FireboltException("Response was null which was not expected.", (int)response.StatusCode,
                        objectResponse.Text, headers, null);

                return objectResponse.Object;
            }
            else
            {
                var objectResponse = await ReadObjectResponseAsync<Error>(response, headers, false, cancellationToken)
                    .ConfigureAwait(false);
                if (objectResponse.Object == null)
                    throw new FireboltException("Response was null which was not expected.", (int)response.StatusCode,
                        objectResponse.Text, headers, null);

                throw new FireboltException<Error>("An unexpected error response", (int)response.StatusCode, objectResponse.Text,
                    headers, objectResponse.Object, null);
            }
        }
        finally
        {
            if (disposeResponse)
                response.Dispose();
        }
    }

    /// <param name="databaseName">Name of the database.</param>
    /// <param name="account"></param>
    /// <param name="baseUrl"></param>
    /// <param name="accessToken"></param>
    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <param name="engine"></param>
    /// <summary>
    ///     Returns engine URL by database name given.
    /// </summary>
    /// <returns>A successful response.</returns>
    /// <exception cref="FireboltException">A server side error occurred.</exception>
    private async Task<GetEngineUrlByDatabaseNameResponse> CoreV1GetEngineUrlByDatabaseNameAsync(string? databaseName,
        string? account, string baseUrl,
        string accessToken, CancellationToken cancellationToken)
    {
        var urlBuilder = new StringBuilder();

        if (account == null)
        {
            urlBuilder.Append(baseUrl.TrimEnd('/'))
                .Append("/core/v1/account/engines:" + "getURLByDatabaseName" + "?")
                .Append(Uri.EscapeDataString("database_name") + "=")
                .Append(Uri.EscapeDataString(ConvertToString(databaseName, CultureInfo.InvariantCulture)));
        }
        else
        {
            var accountId = await GetAccountIdByNameAsync(account, baseUrl, accessToken, cancellationToken);
            if (accountId == null) throw new FireboltException("Account id is missing");
            urlBuilder.Append(baseUrl.TrimEnd('/'))
                .Append("/core/v1/accounts/" + accountId.Account_id + "/engines:" + "getURLByDatabaseName" + "?")
                .Append(Uri.EscapeDataString("database_name") + "=").Append(Uri.EscapeDataString(
                ConvertToString(databaseName, CultureInfo.InvariantCulture)));
        }

        using var request = new HttpRequestMessage();
        request.Method = new HttpMethod("GET");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));
        AddAccessToken(request, accessToken);

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);


        var response = await HttpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        var disposeResponse = true;
        try
        {
            var headers = response.Headers.ToDictionary(h => h.Key, h => h.Value);
            foreach (var item in response.Content.Headers)
                headers[item.Key] = item.Value;
            
            if (response.IsSuccessStatusCode)
            {
                var objectResponse =
                    await ReadObjectResponseAsync<GetEngineUrlByDatabaseNameResponse>(response, headers, false,
                        cancellationToken).ConfigureAwait(false);

                if (objectResponse.Object == null)
                    throw new FireboltException("Response was null which was not expected.", (int)response.StatusCode,
                        objectResponse.Text, headers, null);

                return objectResponse.Object;
            }
            else
            {
                var objectResponse = await ReadObjectResponseAsync<Error>(response, headers, false, cancellationToken)
                    .ConfigureAwait(false);
                if (objectResponse.Object == null)
                    throw new FireboltException("Response was null which was not expected.", (int)response.StatusCode,
                        objectResponse.Text, headers, null);

                throw new FireboltException<Error>("An unexpected error response", (int)response.StatusCode, objectResponse.Text,
                    headers, objectResponse.Object, null);
            }
        }
        finally
        {
            if (disposeResponse)
                response.Dispose();
        }
    }

    private async Task<GetEngineNameByEngineIdResponse> CoreV1GetEngineUrlByEngineNameAsync(string engine,
        string? account, string baseUrl, string accessToken, CancellationToken cancellationToken)
    {
        if (engine == null) throw new FireboltException("Engine name is incorrect or missing");

        var urlBuilder = new StringBuilder();
        var accountName = account ?? "firebolt";
        var accountId = await GetAccountIdByNameAsync(accountName, baseUrl, accessToken, cancellationToken);
        if (accountId == null) throw new FireboltException("Account id is missing");
        urlBuilder.Append(baseUrl.TrimEnd('/'))
            .Append("/core/v1/accounts/" + accountId.Account_id + "/engines:" + "getIdByName" + "?")
            .Append(Uri.EscapeDataString("engine_name") + "=").Append(Uri.EscapeDataString(ConvertToString(
                engine,
                CultureInfo.InvariantCulture)));


        using var request = new HttpRequestMessage();
        request.Method = new HttpMethod("GET");
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

        var url = urlBuilder.ToString();
        request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

        AddAccessToken(request, accessToken);

        var response = await HttpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        try
        {
            var headers = response.Headers.ToDictionary(h => h.Key, h => h.Value);
            foreach (var item in response.Content.Headers)
                headers[item.Key] = item.Value;

            if (response.IsSuccessStatusCode)
            {
                var objectResponse =
                    await ReadObjectResponseAsync<GetEngineNameByEngineIdResponse>(response, headers, false,
                        cancellationToken).ConfigureAwait(false);

                if (objectResponse.Object == null)
                    throw new FireboltException("Response was null which was not expected.", (int)response.StatusCode,
                        objectResponse.Text, headers, null);

                return objectResponse.Object;
            }
            else
            {
                var objectResponse = await ReadObjectResponseAsync<Error>(response, headers, false, cancellationToken)
                    .ConfigureAwait(false);
                if (objectResponse.Object == null)
                    throw new FireboltException("Response was null which was not expected.", (int)response.StatusCode,
                        objectResponse.Text, headers, null);

                throw new FireboltException<Error>("An unexpected error response", (int)response.StatusCode, objectResponse.Text,
                    headers, objectResponse.Object, null);
            }
        }
        finally
        {
            response.Dispose();
        }
    }

    private static void AddAccessToken(HttpRequestMessage request, string? accessToken)
    {
        if (!string.IsNullOrEmpty(accessToken)) request.Headers.Add("Authorization", "Bearer " + accessToken);
    }

    private struct ObjectResponseResult<T>
    {
        public ObjectResponseResult(T responseObject, string responseText)
        {
            Object = responseObject;
            Text = responseText;
        }

        public T Object { get; }

        public string Text { get; }
    }
}