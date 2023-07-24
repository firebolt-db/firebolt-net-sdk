﻿#region License Apache 2.0

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
using System.Net.Http.Headers;
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

    private string? _token;
    private readonly string _endpoint;
    private readonly string _id;
    private readonly string _secret;
    private readonly string _env;

    public FireboltClient(String id, String secret, String endpoint, String? env = null)
    {
        _httpClient = HttpClientSingleton.GetInstance();
        _settings = new Lazy<JsonSerializerSettings>(new JsonSerializerSettings());
        _endpoint = env != null ? $"api.{env}.firebolt.io" : endpoint;
        _id = id;
        _secret = secret;
        _env = env ?? "app";
    }

    private JsonSerializerSettings JsonSerializerSettings => _settings.Value;

    /// <summary>
    ///     Authenticates the user with Firebolt.
    /// </summary>
    /// <param name="id"></param>
    /// <param name="sectet"></param>
    /// <returns></returns>
    private Task<LoginResponse> Login(string id, string secret, string env)
    {
        var credentials = new ServiceAccountLoginRequest(id, secret);
        var url = new UriBuilder()
        {
            Scheme = "https",
            Host = $"id.{env}.firebolt.io",
            Path = Constant.AUTH_SERVICE_ACCOUNT_URL
        }.Uri.ToString();


        return SendAsync<LoginResponse>(HttpMethod.Post, url, credentials.GetFormUrlEncodedContent(), false, CancellationToken.None);
    }

    /// <summary>
    ///     Fetches system engine URL.
    /// </summary>
    /// <param name="accountName">Name of the account</param>
    /// <returns>Engine URL response</returns>
    public async Task<GetSystemEngineUrlResponse> GetSystemEngineUrl(string accountName)
    {
        var url = new UriBuilder()
        {
            Scheme = "https",
            Host = _endpoint,
            Path = String.Format(Constant.ACCOUNT_SYSTEM_ENGINE_URL, accountName)
        }.Uri.ToString();

        try
        {
            return await SendAsync<GetSystemEngineUrlResponse>(HttpMethod.Get, url, (string?)null, true, CancellationToken.None);
        }
        catch (FireboltException e) when (e.StatusCode == HttpStatusCode.NotFound)
        {
            throw new FireboltException(HttpStatusCode.NotFound, $"Account with name {accountName} was not found");
        }
    }

    /// <summary>
    ///     Executes a SQL query
    /// </summary>
    /// <param name="engineEndpoint">Engine endpoint (URL)</param>
    /// <param name="databaseName">Database name</param>
    /// <param name="query">SQL query to execute</param>
    /// <returns>A successful response.</returns>
    public Task<string?> ExecuteQuery(string? engineEndpoint, string? databaseName, string? accountId, string query)
    {
        return ExecuteQueryAsync(engineEndpoint, databaseName, accountId, query,
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
    public Task<string?> ExecuteQuery(string? engineEndpoint, string? databaseName, string? accountId, HashSet<string> setParamList, string query)
    {
        return ExecuteQueryAsync(engineEndpoint, databaseName, accountId, query,
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
    public async Task<string?> ExecuteQueryAsync(string? engineEndpoint, string? databaseName, string? accountId,
                         string query, HashSet<string> setParamList, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(engineEndpoint) || string.IsNullOrEmpty(query))
            throw new FireboltException(
                $"Some parameters are null or empty: engineEndpoint: {engineEndpoint}, databaseName: {databaseName} or query: {query}");

        var setParams = setParamList.Aggregate(string.Empty, (current, item) => current + "&" + item);
        var urlBuilder = new UriBuilder(engineEndpoint)
        {
            Scheme = "https",
            Port = -1
        };

        var parameters = new Dictionary<string, string>
    {
        { "output_format", "JSON_Compact" }
    };
        if (databaseName != null)
        {
            parameters["database"] = databaseName;
        }
        if (accountId != null)
        {
            parameters["account_id"] = accountId;
        }
        var queryStr = parameters.Aggregate(new StringBuilder(),
                  (q, param) => q.AppendFormat("{0}{1}={2}",
                               q.Length > 0 ? "&" : "", param.Key, param.Value));
        if (setParams.Length > 0)
            queryStr.Append("&").Append(setParams);
        urlBuilder.Query = queryStr.ToString();
        var url = urlBuilder.Uri.ToString();

        return await SendAsync<string>(HttpMethod.Post, url, query, "text/plain", true, cancellationToken);
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
    public async Task<GetAccountIdByNameResponse> GetAccountIdByNameAsync(string account,
        CancellationToken cancellationToken)
    {

        var url = new UriBuilder()
        {
            Scheme = "https",
            Host = _endpoint,
            Path = String.Format(Constant.ACCOUNT_BY_NAME_URL, account)
        }.Uri.ToString();

        return await SendAsync<GetAccountIdByNameResponse>(HttpMethod.Get, url, (string?)null, true, cancellationToken);
    }

    private async Task<T> SendAsync<T>(HttpMethod method, string uri, string? body, bool requiresAuth,
        CancellationToken cancellationToken)
    {
        return await SendAsync<T>(method, uri, body, "application/json", requiresAuth, cancellationToken);
    }

    private async Task<T> SendAsync<T>(HttpMethod method, string uri, string? body, string bodyType,
        bool needsAccessToken, CancellationToken cancellationToken)
    {
        if (body != null)
        {
            var content = new StringContent(body);
            content.Headers.ContentType = MediaTypeHeaderValue.Parse(bodyType);
            return await SendAsync<T>(method, uri, content, needsAccessToken, cancellationToken);
        }
        return await SendAsync<T>(method, uri, (HttpContent?)null, needsAccessToken, cancellationToken);
    }

    private async Task<T> SendAsync<T>(HttpMethod method, string uri, HttpContent? content, bool needsAccessToken, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage();
        request.Method = method;
        request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));
        request.Content = content;
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
                    throw new FireboltException("Response was null which was not expected.", HttpStatusCode.OK,
                        objectResponse.Text, headers, null);

                return objectResponse.Object;
            }
            else
            {
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



    private void AddAccessToken(HttpRequestMessage request)
    {
        if (string.IsNullOrEmpty(_token))
        {
            throw new FireboltException("The Access token is null or empty - EstablishConnection must be called");
        }
        request.Headers.Add("Authorization", "Bearer " + _token);
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
        LoginResponse token;
        var storedToken = await TokenSecureStorage.GetCachedToken(_id, _secret);
        if (storedToken != null)
        {
            token = new LoginResponse(access_token: storedToken.token,
                expires_in: storedToken.expiration.ToString(),
                token_type: "");
        }
        else
        {
            token = await Login(_id, _secret, _env);
            await TokenSecureStorage.CacheToken(token, _id, _secret);
        }
        _token = token.Access_token;
    }
}
