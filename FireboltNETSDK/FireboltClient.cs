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
using FireboltDotNetSdk.Utils;
using Newtonsoft.Json;
using static FireboltDotNetSdk.Client.FireRequest;
using static FireboltDotNetSdk.Client.FireResponse;

namespace FireboltDotNetSdk;

public class FireboltClient
{

    private readonly Lazy<JsonSerializerSettings> _settings;
    private readonly HttpClient _httpClient;

    private Token? _loginToken;
    private readonly string _endpoint;
    private readonly string _username;
    private readonly string _password;

    public FireboltClient(String username, String password, String endpoint)
    {
        _httpClient = HttpClientSingleton.GetInstance();
        _settings = new Lazy<JsonSerializerSettings>(new JsonSerializerSettings());
        _endpoint = endpoint;
        _username = username;
        _password = password;
    }

    private JsonSerializerSettings JsonSerializerSettings => _settings.Value;

    /// <summary>
    ///     Authenticates the user with Firebolt.
    /// </summary>
    /// <param name="loginRequest"></param>
    /// <returns></returns>
    private Task<LoginResponse> Login(LoginRequest loginRequest)
    {
        return AuthV1LoginAsync(loginRequest, CancellationToken.None);
    }

    /// <summary>
    ///     Returns engine URL by database name given.
    /// </summary>
    /// <param name="databaseName">Name of the database.</param>
    /// <param name="account"></param>
    /// <returns>A successful response.</returns>
    public Task<GetEngineUrlByDatabaseNameResponse> GetEngineUrlByDatabaseName(string? databaseName,
        string? account)
    {
        return CoreV1GetEngineUrlByDatabaseNameAsync(databaseName, account, CancellationToken.None);
    }

    /// <summary>
    ///     Returns engine URL by engine name given.
    /// </summary>
    /// <param name="engine">Name of the engine.</param>
    /// <param name="account">Name of the account</param>
    /// <returns>A successful response.</returns>
    public Task<GetEngineNameByEngineIdResponse> GetEngineUrlByEngineName(string engine, string? account)
    {
        return CoreV1GetEngineUrlByEngineNameAsync(engine, account, CancellationToken.None);
    }

    /// <summary>
    ///     Returns engine URL by engine id.
    /// </summary>
    /// <param name="engineId">Name of the database.</param>
    /// <param name="accountId"></param>
    /// <returns>An Engine url response.</returns>
    public Task<GetEngineUrlByEngineNameResponse> GetEngineUrlByEngineId(string engineId, string accountId)
    {
        return CoreV1GetEngineUrlByEngineIdAsync(engineId, accountId, CancellationToken.None);
    }

    /// <summary>
    ///     Executes a SQL query
    /// </summary>
    /// <param name="engineEndpoint">Engine endpoint (URL)</param>
    /// <param name="databaseName">Database name</param>
    /// <param name="query">SQL query to execute</param>
    /// <returns>A successful response.</returns>
    public Task<string?> ExecuteQuery(string? engineEndpoint, string databaseName, string query)
    {
        return ExecuteQueryAsync(engineEndpoint, databaseName, query,
            new HashSet<string>(), CancellationToken.None);
    }

    /// <summary>
    ///     Executes a SQL query
    /// </summary>
    /// <param name="engineEndpoint">Engine endpoint (URL)</param>
    /// <param name="databaseName">Database name</param>
    /// <param name="setParamList">parameters</param>
    /// <param name="query">SQL query to execute</param>
    /// <returns>A successful response.</returns>
    public Task<string?> ExecuteQuery(string? engineEndpoint, string databaseName, HashSet<string> setParamList, string query)
    {
        return ExecuteQueryAsync(engineEndpoint, databaseName, query,
            setParamList, CancellationToken.None);
    }

    /// <param name="engineEndpoint">Engine endpoint (URL)</param>
    /// <param name="databaseName">Database name</param>
    /// <param name="query">SQL query to execute</param>
    /// <param name="setParamList"></param>
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
        HashSet<string> setParamList, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(engineEndpoint) || string.IsNullOrEmpty(databaseName) ||
            string.IsNullOrEmpty(query))
            throw new FireboltException(
                $"Some parameters are null or empty: engineEndpoint: {engineEndpoint}, databaseName: {databaseName} or query: {query}");

        var setParam = setParamList.Aggregate(string.Empty, (current, item) => current + "&" + item);
        var urlBuilder = new StringBuilder();
        urlBuilder.Append("https://").Append(engineEndpoint).Append("?database=").Append(databaseName)
            .Append(setParam).Append("&output_format=JSONCompact");
        return await SendAsync<string>(HttpMethod.Post, urlBuilder.ToString(), query, "text/plain", true, cancellationToken);
    }

    private async Task<ObjectResponseResult<T?>> ReadObjectResponseAsync<T>(HttpResponseMessage? response,
        IReadOnlyDictionary<string, IEnumerable<string>> headers, bool readResponseAsString, CancellationToken cancellationToken)
    {
        if (response == null) return new ObjectResponseResult<T?>(default, string.Empty);
        if (typeof(T) == typeof(string))
        {
            var text = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            return new ObjectResponseResult<T?>((T)(object)text, text);
        }

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
        else
        {
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
    }

    /// <param name="account"></param>
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
        CancellationToken cancellationToken)
    {
        if (account == null) throw new FireboltException("Account name is empty");

        var urlBuilder = new StringBuilder();
        urlBuilder.Append(_endpoint)
            .Append("/iam/v2/accounts:getIdByName?");
        urlBuilder.Append(Uri.EscapeDataString("accountName") + "=")
            .Append(Uri.EscapeDataString(ConvertToString(account, CultureInfo.InvariantCulture)));

        return await SendAsync<GetAccountIdByNameResponse>(HttpMethod.Get, urlBuilder.ToString(), null, true, cancellationToken);
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
    private async Task<LoginResponse> AuthV1LoginAsync(LoginRequest body, CancellationToken cancellationToken)
    {
        if (body == null)
            throw new ArgumentNullException(nameof(body));

        return await SendAsync<LoginResponse>(HttpMethod.Post, _endpoint + "/auth/v1/login", JsonConvert.SerializeObject(body, _settings.Value), false, cancellationToken);
    }

    private async Task<GetEngineUrlByEngineNameResponse> CoreV1GetEngineUrlByEngineIdAsync(string engineId,
        string accountId, CancellationToken cancellationToken)
    {
        if (engineId == null) throw new FireboltException("Engine name is incorrect or missing");

        var urlBuilder = new StringBuilder();
        urlBuilder.Append(_endpoint).Append("/core/v1/accounts/" + accountId + "/engines/" + engineId);
        return await SendAsync<GetEngineUrlByEngineNameResponse>(HttpMethod.Get, urlBuilder.ToString(), null, true, cancellationToken);
    }

    /// <param name="databaseName">Name of the database.</param>
    /// <param name="account"></param>
    /// <param name="cancellationToken">
    ///     A cancellation token that can be used by other objects or threads to receive notice of
    ///     cancellation.
    /// </param>
    /// <summary>
    ///     Returns engine URL by database name given.
    /// </summary>
    /// <returns>A successful response.</returns>
    /// <exception cref="FireboltException">A server side error occurred.</exception>
    private async Task<GetEngineUrlByDatabaseNameResponse> CoreV1GetEngineUrlByDatabaseNameAsync(string? databaseName,
        string? account, CancellationToken cancellationToken)
    {
        var urlBuilder = new StringBuilder();

        if (account == null)
        {
            urlBuilder.Append(_endpoint).Append("/core/v1/account/engines:" + "getURLByDatabaseName" + "?")
                .Append(Uri.EscapeDataString("database_name") + "=")
                .Append(Uri.EscapeDataString(ConvertToString(databaseName, CultureInfo.InvariantCulture)));
        }
        else
        {
            var accountId = await GetAccountIdByNameAsync(account, cancellationToken);
            if (accountId == null) throw new FireboltException("Account id is missing");
            urlBuilder.Append(_endpoint).Append("/core/v1/accounts/" + accountId.Account_id + "/engines:" + "getURLByDatabaseName" + "?")
                .Append(Uri.EscapeDataString("database_name") + "=").Append(Uri.EscapeDataString(
                ConvertToString(databaseName, CultureInfo.InvariantCulture)));
        }
        return await SendAsync<GetEngineUrlByDatabaseNameResponse>(HttpMethod.Get, urlBuilder.ToString(), null, true, cancellationToken);
    }

    private async Task<GetEngineNameByEngineIdResponse> CoreV1GetEngineUrlByEngineNameAsync(string engine,
        string? account, CancellationToken cancellationToken)
    {
        if (engine == null) throw new FireboltException("Engine name is incorrect or missing");

        var accountName = account ?? "firebolt";
        var accountId = await GetAccountIdByNameAsync(accountName, cancellationToken);
        if (accountId == null) throw new FireboltException("Account id is missing");

        var urlBuilder = new StringBuilder();
        urlBuilder.Append(_endpoint).Append("/core/v1/accounts/" + accountId.Account_id + "/engines:" + "getIdByName" + "?")
            .Append(Uri.EscapeDataString("engine_name") + "=").Append(Uri.EscapeDataString(ConvertToString(
                engine,
                CultureInfo.InvariantCulture)));

        return await SendAsync<GetEngineNameByEngineIdResponse>(HttpMethod.Get, urlBuilder.ToString(), null, true, cancellationToken);
    }

    private async Task<T> SendAsync<T>(HttpMethod method, string uri, string? body, bool requiresAuth,
        CancellationToken cancellationToken)
    {
        return await SendAsync<T>(method, uri, body, "application/json", requiresAuth, cancellationToken);
    }

    private async Task<T> SendAsync<T>(HttpMethod method, string uri, string? body, string bodyType, bool needsAccessToken, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage();
        request.Method = method;
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse(bodyType));

        if (body != null)
        {
            var content = new StringContent(body);
            content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json");
            request.Content = content;
        }

        request.RequestUri = new Uri(uri, UriKind.RelativeOrAbsolute);

        if (needsAccessToken)
        {
            AddAccessToken(request);
        }

        var response = await _httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);

        try
        {
            var headers = response.Headers.ToDictionary(h => h.Key, h => h.Value);
            foreach (var item in response.Content.Headers)
                headers[item.Key] = item.Value;

            if (response.IsSuccessStatusCode)
            {
                var objectResponse = await ReadObjectResponseAsync<T>(response, headers, false, cancellationToken).ConfigureAwait(false);
                if (objectResponse.Object == null)
                    throw new FireboltException("Response was null which was not expected.", (int)HttpStatusCode.OK,
                        objectResponse.Text, headers, null);

                return objectResponse.Object;
            }
            else
            {
                String? errorMessage;
                try
                {
                    errorMessage = (await ReadObjectResponseAsync<ResponseError?>(response, headers, true, cancellationToken)
                            .ConfigureAwait(false)).Object?.message;
                }
                catch (FireboltException exception)
                {
                    //If we are unable to parse the exception then we return an error containing only the status code from the server
                    if (exception.GetBaseException().GetType() != typeof(JsonReaderException))
                    {
                        throw exception;
                    }
                    else
                    {
                        errorMessage = exception.Response;
                    }
                }

                if (errorMessage != null)
                {
                    throw new FireboltException($"Received an unexpected status code from the server: {(int)response.StatusCode} with the response: {errorMessage} ");
                }
                else
                {
                    throw new FireboltException($"Received an unexpected status code from the server: {(int)response.StatusCode}");
                }
            }
        }
        finally
        {
            response.Dispose();
        }
    }



    private void AddAccessToken(HttpRequestMessage request)
    {
        if (string.IsNullOrEmpty(_loginToken?.AccessToken))
        {
            throw new FireboltException("The Access token is null or empty - EstablishConnection must be called");
        }
        request.Headers.Add("Authorization", "Bearer " + _loginToken.AccessToken);
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

    private struct ObjectResponseResult<T>
    {
        public ObjectResponseResult(T? responseObject, string? responseText)
        {
            Object = responseObject;
            Text = responseText;
        }

        public T? Object { get; }

        public string? Text { get; }
    }

    public async Task EstablishConnection()
    {
        try
        {
            LoginResponse token;
            var storedToken = await TokenSecureStorage.GetCachedToken(_username, _password);
            if (storedToken != null)
            {
                token = new LoginResponse
                {
                    Access_token = storedToken.token,
                    Expires_in = storedToken.expiration.ToString()
                };
            }
            else
            {
                var credentials = new LoginRequest
                {
                    Password = _password,
                    Username = _username
                };
                token = await Login(credentials);
                await TokenSecureStorage.CacheToken(token, _username, _password);
            }
            _loginToken = new Token(token.Access_token, token.Refresh_token, token.Expires_in);
        }
        catch (FireboltException ex)
        {
            throw new FireboltException(ex.Message);
        }
    }

    public class Token
    {
        public Token(string? accessToken, string? refreshToken, string? tokenExpired)
        {
            AccessToken = accessToken;
            RefreshToken = refreshToken;
            TokenExpired = tokenExpired;
        }

        public string? AccessToken { get; }
        public string? RefreshToken { get; }
        public string? TokenExpired { get; }
    }
}