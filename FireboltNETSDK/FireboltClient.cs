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
    private readonly HttpMessageInvoker _httpClient;

    private Token? _loginToken;
    private readonly string _endpoint;
    private readonly string _username;
    private readonly string _password;
    private readonly string _jsonContentType = "application/json";
    private readonly string _textContentType = "text/plain";

    public FireboltClient(String username, String password, String endpoint, HttpMessageInvoker httpClient)
    {
        _httpClient = httpClient;
        _settings = new Lazy<JsonSerializerSettings>(new JsonSerializerSettings());
        _endpoint = endpoint;
        _username = username;
        _password = password;
    }

    private JsonSerializerSettings JsonSerializerSettings => _settings.Value;

    /// <summary>
    ///     Authenticates the user with Firebolt.
    /// </summary>
    /// <param name="username"></param>
    /// <param name="password"></param>
    /// <returns></returns>
    private Task<LoginResponse> Login(string username, string password)
    {
        if (IsServiceAccount(username))
        {
            var credentials = new UsernamePasswordLoginRequest(username, password);
            return SendAsync<LoginResponse>(HttpMethod.Post, _endpoint + Constant.AUTH_USERNAME_PASSWORD_URL, JsonConvert.SerializeObject(credentials, _settings.Value), _jsonContentType, false, CancellationToken.None, false);
        }
        else
        {
            var credentials = new ServiceAccountLoginRequest(username, password);
            return SendAsync<LoginResponse>(HttpMethod.Post, _endpoint + Constant.AUTH_SERVICE_ACCOUNT_URL, credentials.GetFormUrlEncodedContent(), false, CancellationToken.None, false);
        }
    }

    private bool IsServiceAccount(string username)
    {
        return username.Contains("@");
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
    ///     Returns engine id by engine name.
    /// </summary>
    /// <param name="engine">Name of the engine.</param>
    /// <param name="account">Name of the account</param>
    /// <returns>A successful response.</returns>
    public Task<GetEngineIdByEngineNameResponse> GetEngineIdByEngineName(string engine, string? account)
    {
        return CoreV1GetEngineByEngineNameAsync(engine, account, CancellationToken.None);
    }

    /// <summary>
    ///     Returns engine URL by engine id.
    /// </summary>
    /// <param name="engineId">Name of the database.</param>
    /// <param name="accountId"></param>
    /// <returns>An Engine url response.</returns>
    public Task<GetEngineUrlByEngineNameResponse> GetEngineUrlByEngineId(string? engineId, string? accountId)
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
            .Append(setParam).Append("&output_format=JSON_Compact");
        return await SendAsync<string>(HttpMethod.Post, urlBuilder.ToString(), query, _textContentType, true, cancellationToken, true);
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
                throw new FireboltException(message, response.StatusCode, responseText, headers, exception);
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
                throw new FireboltException(message, response.StatusCode, string.Empty, headers, exception);
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
    public async Task<GetAccountIdByNameResponse> GetAccountIdByNameAsync(string? account,
        CancellationToken cancellationToken)
    {
        if (account == null) throw new FireboltException("Account name is empty");

        var urlBuilder = new StringBuilder();
        urlBuilder.Append(_endpoint)
            .Append("/iam/v2/accounts:getIdByName?");
        urlBuilder.Append(Uri.EscapeDataString("accountName") + "=")
            .Append(Uri.EscapeDataString(ConvertToString(account, CultureInfo.InvariantCulture)));

        return await SendAsync<GetAccountIdByNameResponse>(HttpMethod.Get, urlBuilder.ToString(), (string?)null, true, cancellationToken);
    }

    private async Task<GetEngineUrlByEngineNameResponse> CoreV1GetEngineUrlByEngineIdAsync(string? engineId,
        string? accountId, CancellationToken cancellationToken)
    {
        if (engineId == null) throw new FireboltException("Engine name is incorrect or missing");
        if (accountId == null) throw new FireboltException("Account id is incorrect or missing");

        var urlBuilder = new StringBuilder();
        urlBuilder.Append(_endpoint).Append("/core/v1/accounts/" + accountId + "/engines/" + engineId);
        return await SendAsync<GetEngineUrlByEngineNameResponse>(HttpMethod.Get, urlBuilder.ToString(), (string?)null, true, cancellationToken);
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
        return await SendAsync<GetEngineUrlByDatabaseNameResponse>(HttpMethod.Get, urlBuilder.ToString(), (string?)null, true, cancellationToken);
    }

    private async Task<GetEngineIdByEngineNameResponse> CoreV1GetEngineByEngineNameAsync(string engine,
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

        return await SendAsync<GetEngineIdByEngineNameResponse>(HttpMethod.Get, urlBuilder.ToString(), (string?)null, true, cancellationToken);
    }

    private async Task<T> SendAsync<T>(HttpMethod method, string uri, string? body, bool requiresAuth,
        CancellationToken cancellationToken)
    {
        return await SendAsync<T>(method, uri, body, _jsonContentType, requiresAuth, cancellationToken, true);
    }

    private async Task<T> SendAsync<T>(HttpMethod method, string uri, string? body, string bodyType,
        bool needsAccessToken, CancellationToken cancellationToken, bool retryUnauthorized)
    {
        if (body != null)
        {
            var content = new StringContent(body);
            content.Headers.ContentType = MediaTypeHeaderValue.Parse(bodyType);
            return await SendAsync<T>(method, uri, content, needsAccessToken, cancellationToken, retryUnauthorized);
        }
        return await SendAsync<T>(method, uri, (HttpContent?)null, needsAccessToken, cancellationToken, retryUnauthorized);
    }

    private async Task<T> SendAsync<T>(HttpMethod method, string uri, HttpContent? content, bool needsAccessToken, CancellationToken cancellationToken, bool retryUnauthorized)
    {
        using var request = new HttpRequestMessage();
        request.Method = method;
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));
        request.Content = content;
        request.RequestUri = new Uri(uri, UriKind.RelativeOrAbsolute);

        //Add access token only when it is required for the request
        if (needsAccessToken)
        {
            AddAccessToken(request);
        }

        var response = await _httpClient
            .SendAsync(request, cancellationToken)
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
                    throw new FireboltException("Response was null which was not expected.", HttpStatusCode.OK,
                        objectResponse.Text, headers, null);

                return objectResponse.Object;
            }
            else
            {
                if (needsAccessToken && response.StatusCode == HttpStatusCode.Unauthorized && retryUnauthorized)
                {
                    //If we need an access token and the token is invalid (401), we try to re-establish the connection once.
                    await EstablishConnection(true);
                    return await SendAsync<T>(method, uri, content, needsAccessToken, cancellationToken, false);
                }
                string? errorResponse = null;
                try
                {
                    errorResponse = (await ReadObjectResponseAsync<ResponseError?>(response, headers, true, cancellationToken)
                        .ConfigureAwait(false)).Object?.message;
                }
                catch (FireboltException exception)
                {
                    if (exception.GetBaseException().GetType() == typeof(JsonReaderException))
                    {
                        errorResponse = exception.Response;
                    }
                }
                throw new FireboltException(response.StatusCode, errorResponse);
            }
        }
        finally
        {
            response.Dispose();
        }
    }



    private async void AddAccessToken(HttpRequestMessage request)
    {
        var token = _loginToken;
        if (string.IsNullOrEmpty(token?.AccessToken))
        {
            token = await EstablishConnection();
        }
        request.Headers.Add("Authorization", "Bearer " + token.AccessToken);
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

    public async Task<Token> EstablishConnection()
    {
        return await EstablishConnection(false);
    }

    private async Task<Token> EstablishConnection(bool forceTokenRefresh)
    {
        LoginResponse loginResponse;
        var storedToken = forceTokenRefresh ? null : await TokenSecureStorage.GetCachedToken(_username, _password);
        if (storedToken != null)
        {
            loginResponse = new LoginResponse(access_token: storedToken.token,
                expires_in: storedToken.expiration.ToString(),
                refresh_token: "",
                token_type: "");
        }
        else
        {
            loginResponse = await Login(_username, _password);
            await TokenSecureStorage.CacheToken(loginResponse, _username, _password);
        }
        _loginToken = new Token(loginResponse.Access_token, loginResponse.Refresh_token, loginResponse.Expires_in);
        return _loginToken;
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