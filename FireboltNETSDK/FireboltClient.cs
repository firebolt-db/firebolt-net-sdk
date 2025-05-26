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
using System.Net.Http.Headers;
using System.Text;
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;
using Newtonsoft.Json;
using FireboltDotNetSdk.Client;
using static FireboltDotNetSdk.Client.FireResponse;
using FireboltNETSDK.Exception;

namespace FireboltDotNetSdk;

public abstract class FireboltClient
{

    private readonly Lazy<JsonSerializerSettings> _settings;
    private readonly HttpClient _httpClient;

    private string? _token;
    protected readonly string _endpoint;
    protected readonly FireboltConnection _connection;
    protected readonly string _id;
    protected readonly string _secret;
    protected readonly string _env;
    public string? _protocolVersion;

    protected readonly string _jsonContentType = "application/json";
    private readonly string _textContentType = "text/plain";
    private readonly string HEADER_PROTOCOL_VERSION = "Firebolt-Protocol-Version";
    private readonly string HEADER_UPDATE_PARAMETER = "Firebolt-Update-Parameters";
    private readonly string HEADER_UPDATE_ENDPOINT = "Firebolt-Update-Endpoint";
    private readonly string HEADER_RESET_SESSION = "Firebolt-Reset-Session";

    private IDictionary<string, string> queryParameters = new Dictionary<string, string>();
    internal readonly TokenStorage _tokenStorage;

    protected FireboltClient(FireboltConnection connection, string id, string secret, string endpoint, string? env, string? protocolVersion, HttpClient httpClient)
    {
        _httpClient = httpClient;
        _settings = new Lazy<JsonSerializerSettings>(new JsonSerializerSettings());
        _endpoint = env != null ? $"api.{env}.firebolt.io" : endpoint;
        _connection = connection;
        _id = id;
        _secret = secret;
        _env = env ?? "app";
        _protocolVersion = protocolVersion;
        _tokenStorage = TokenStorage.create(connection.TokenStorageType);
    }

    private JsonSerializerSettings JsonSerializerSettings => _settings.Value;

    protected abstract Task<LoginResponse> Login(string id, string secret, string env);

    /// <summary>
    ///     Executes a SQL query
    /// </summary>
    /// <param name="engineEndpoint">Engine endpoint (URL)</param>
    /// <param name="databaseName">Database name</param>
    /// <param name="query">SQL query to execute</param>
    /// <returns>A successful response.</returns>
    public Task<string> ExecuteQuery(string? engineEndpoint, string? databaseName, string? accountId, string query)
    {
        return ExecuteQuery(engineEndpoint, databaseName, accountId, new HashSet<string>(), query);
    }

    /// <summary>
    ///     Executes a SQL query
    /// </summary>
    /// <param name="engineEndpoint">Engine endpoint (URL)</param>
    /// <param name="databaseName">Database name</param>
    /// <param name="setParamList">parameters</param>
    /// <param name="query">SQL query to execute</param>
    /// <returns>A successful response.</returns>
    public virtual Task<string> ExecuteQuery(string? engineEndpoint, string? databaseName, string? accountId, HashSet<string> setParamList, string query)
    {
        return ExecuteQueryAsync<string>(engineEndpoint, databaseName, accountId, query,
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
    public virtual async Task<T> ExecuteQueryAsync<T>(string? engineEndpoint, string? databaseName, string? accountId,
                         string query, HashSet<string> setParamList, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(engineEndpoint) || string.IsNullOrEmpty(query))
            throw new FireboltException(
                $"Some parameters are null or empty: engineEndpoint: {engineEndpoint} or query: {query}");

        var url = GetUrl(engineEndpoint, databaseName, accountId, setParamList);

        return await SendAsync<T>(HttpMethod.Post, url, query, _textContentType, needsAccessToken: true, retryUnauthorized: true, cancellationToken);
    }

    private string GetUrl(string engineEndpoint, string? databaseName, string? accountId, HashSet<string> setParamList)
    {
        var setParams = setParamList.Aggregate(string.Empty, (current, item) => current + "&" + item);
        var urlBuilder = new UriBuilder(engineEndpoint)
        {
            Scheme = "https",
            Port = -1
        };

        var queryStr = GetQueryString(databaseName, accountId);
        if (setParams.Length > 0)
            queryStr.Append('&').Append(setParams);
        urlBuilder.Query = queryStr.ToString();
        var url = urlBuilder.Uri.ToString();
        return url;
    }

    private StringBuilder GetQueryString(string? databaseName, string? accountId, string outputFormat = "JSON_Compact")
    {
        var parameters = new Dictionary<string, string>() { { "output_format", outputFormat } };
        if (databaseName != null)
        {
            parameters["database"] = databaseName;
        }
        if ((_connection.IsSystem || _connection.InfraVersion >= 2) && accountId != null)
        {
            parameters["account_id"] = accountId;
        }
        if (!_connection.IsSystem && _connection.InfraVersion >= 2 && _connection.EngineName != null)
        {
            parameters["engine"] = _connection.EngineName;
        }
        if (!_connection.IsSystem && _connection.InfraVersion >= 2)
        {
            parameters["query_label"] = Guid.NewGuid().ToString();
        }
        foreach (var item in queryParameters)
        {
            parameters[item.Key] = item.Value;
        }
        var queryStr = parameters.Aggregate(new StringBuilder(),
            (q, param) => q.AppendFormat("{0}{1}={2}",
                q.Length > 0 ? "&" : "", param.Key, param.Value));
        return queryStr;
    }

    private async Task<ObjectResponseResult<T>> ReadObjectResponseAsync<T>(HttpResponseMessage? response,
        IReadOnlyDictionary<string, IEnumerable<string>> headers, bool readResponseAsString, CancellationToken cancellationToken)
    {
        if (response == null) return new ObjectResponseResult<T>(default, string.Empty);
        if (typeof(T) == typeof(string))
        {
            var text = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            return new ObjectResponseResult<T>((T)(object)text, text);
        }

        if (readResponseAsString)
        {
            var responseText = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var typedBody = JsonConvert.DeserializeObject<T>(responseText, JsonSerializerSettings);
                return new ObjectResponseResult<T>(typedBody, responseText);
            }
            catch (JsonException exception)
            {
                var message = "Could not deserialize the response body string as " + typeof(T).FullName + ".";
                throw new FireboltException(message, response.StatusCode, responseText, headers, exception);
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
            return new ObjectResponseResult<T>(typedBody, string.Empty);
        }
        catch (JsonException exception)
        {
            var message = "Could not deserialize the response body stream as " + typeof(T).FullName + ".";
            throw new FireboltException(message, response.StatusCode, string.Empty, headers, exception);
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
    public abstract Task<GetAccountIdByNameResponse> GetAccountIdByNameAsync(string account, CancellationToken cancellationToken);

    protected async Task<T> GetJsonResponseAsync<T>(HttpMethod method, string uri, string? body, bool requiresAuth,
        CancellationToken cancellationToken)
    {
        return await SendAsync<T>(method, uri, body, _jsonContentType, requiresAuth, retryUnauthorized: true, cancellationToken);
    }

    protected async Task<T> SendAsync<T>(HttpMethod method, string uri, string? body, string bodyType,
        bool needsAccessToken, bool retryUnauthorized, CancellationToken cancellationToken)
    {
        if (body != null)
        {
            var content = new StringContent(body);
            content.Headers.ContentType = MediaTypeHeaderValue.Parse(bodyType);
            return await SendAsync<T>(method, uri, content, needsAccessToken, retryUnauthorized, cancellationToken);
        }
        return await SendAsync<T>(method, uri, content: null, needsAccessToken, retryUnauthorized, cancellationToken);
    }

    protected async Task<T> SendAsync<T>(HttpMethod method, string uri, HttpContent? content, bool needsAccessToken, bool retryUnauthorized, CancellationToken cancellationToken)
    {
        using var request = await GetHttpRequest(method, uri, content, needsAccessToken);

        var response = await _httpClient
            .SendAsync(request, cancellationToken)
            .ConfigureAwait(false);

        try
        {
            ProcessResponseHeaders(response.Headers, cancellationToken);
            var headers = response.Headers.ToDictionary(h => h.Key, h => h.Value);
            foreach (var item in response.Content.Headers)
                headers[item.Key] = item.Value;

            await ThrowIfErroredResponse(response, headers, cancellationToken);

            if (response.IsSuccessStatusCode)
            {
                var objectResponse = await ReadObjectResponseAsync<T>(response, headers, readResponseAsString: false, cancellationToken).ConfigureAwait(false);

                return objectResponse.Object ?? throw new FireboltException("Response was null which was not expected.",
                    HttpStatusCode.OK,
                    objectResponse.Text, headers, null);
            }
            else
            {
                if (!needsAccessToken || response.StatusCode != HttpStatusCode.Unauthorized || !retryUnauthorized)
                    throw new FireboltException(response.StatusCode,
                        await GetErrorMessage(response, headers, cancellationToken));

                //If we need an access token and the token is invalid (401), we try to re-establish the connection once.
                await EstablishConnection(forceTokenRefresh: true);
                return await SendAsync<T>(method, uri, content, needsAccessToken, retryUnauthorized: false, cancellationToken);
            }
        }
        finally
        {
            response.Dispose();
        }
    }

    private async Task<string?> GetErrorMessage(HttpResponseMessage response,
        Dictionary<string, IEnumerable<string>> headers, CancellationToken cancellationToken)
    {
        string? errorResponse = null;
        try
        {
            errorResponse = (await ReadObjectResponseAsync<ResponseError>(response, headers, readResponseAsString: true, cancellationToken)
                .ConfigureAwait(false)).Object?.message;
        }
        catch (FireboltException exception)
        {
            if (exception.GetBaseException().GetType() == typeof(JsonReaderException))
            {
                errorResponse = exception.Response;
            }
        }
        return errorResponse;
    }

    private async Task ThrowIfErroredResponse(HttpResponseMessage response,
        Dictionary<string, IEnumerable<string>> headers,
        CancellationToken cancellationToken)
    {
        try
        {
            var anyResponse = await ReadObjectResponseAsync<JsonErrorQueryResult>(response, headers, readResponseAsString: true, cancellationToken).ConfigureAwait(false);
            if (anyResponse.Object?.Errors != null)
            {
                throw new FireboltStructuredException(anyResponse.Object.Errors);
            }
        }
        catch (FireboltStructuredException)
        {
            throw;
        }
        catch (System.Exception)
        {
            // Ignore any other parsing exceptions, we will handle them later.
        }
    }

    private async Task<HttpRequestMessage> GetHttpRequest(HttpMethod method, string uri, HttpContent? content, bool needsAccessToken)
    {
        HttpRequestMessage? request = null;
        try
        {
            request = new HttpRequestMessage();
            request.Method = method;
            request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse(_jsonContentType));
            if (_protocolVersion != null)
            {
                request.Headers.Add(HEADER_PROTOCOL_VERSION, _protocolVersion);
            }
            request.Content = content;
            request.RequestUri = new Uri(uri, UriKind.RelativeOrAbsolute);
            //Add access token only when it is required for the request
            if (needsAccessToken)
            {
                if (string.IsNullOrEmpty(_token))
                {
                    _token = await EstablishConnection();
                }
                request.Headers.Add("Authorization", "Bearer " + _token);
            }
            return request;
        }
        catch
        {
            request?.Dispose();
            throw;
        }
    }

    private void ProcessResponseHeaders(HttpResponseHeaders headers, CancellationToken cancellationToken)
    {
        FireboltConnectionStringBuilder connectionBuilder = new FireboltConnectionStringBuilder(_connection.ConnectionString);
        bool shouldUpdateConnection = ProcessResetSession(headers);
        List<string[]> parameters = new List<string[]>();
        shouldUpdateConnection = ExtractParameters(headers, connectionBuilder, parameters, shouldUpdateConnection);
        shouldUpdateConnection = ProcessParameters(connectionBuilder, parameters, shouldUpdateConnection);

        if (shouldUpdateConnection)
        {
            _connection.UpdateConnectionSettings(connectionBuilder, cancellationToken);
        }
    }

    private bool ProcessResetSession(HttpResponseHeaders headers)
    {
        if (queryParameters.Count() > 0 && headers.Contains(HEADER_RESET_SESSION))
        {
            queryParameters.Clear();
            return true;
        }
        return false;
    }

    private bool ExtractParameters(HttpResponseHeaders headers, FireboltConnectionStringBuilder connectionBuilder, List<string[]> parameters, bool shouldUpdateConnection)
    {
        if (headers.Contains(HEADER_UPDATE_ENDPOINT))
        {
            string? endpointHeader = headers.GetValues(HEADER_UPDATE_ENDPOINT).FirstOrDefault();
            if (endpointHeader != null)
            {
                string[] enpointHeaderParts = endpointHeader.Split('?', 2, StringSplitOptions.TrimEntries);
                if (!enpointHeaderParts[0].Equals(connectionBuilder.Endpoint))
                {
                    connectionBuilder.Endpoint = enpointHeaderParts[0];
                    shouldUpdateConnection = true;
                }
                if (enpointHeaderParts.Length > 1)
                {
                    parameters.AddRange(enpointHeaderParts[1].Split("&").Select(p => p.Split('=', 2, StringSplitOptions.TrimEntries)).ToList());
                }
            }
        }
        if (headers.Contains(HEADER_UPDATE_PARAMETER))
        {
            shouldUpdateConnection = true;
            parameters.AddRange(headers.GetValues(HEADER_UPDATE_PARAMETER).Select(p => p.Split('=', 2, StringSplitOptions.TrimEntries)).ToList());
        }
        return shouldUpdateConnection;
    }

    protected List<string[]> ExtractParameters(string queryString)
    {
        return queryString.Split("&").Select(p => p.Split('=', 2, StringSplitOptions.TrimEntries)).ToList();
    }

    protected bool ProcessParameters(FireboltConnectionStringBuilder connectionBuilder, List<string[]> parameters, bool shouldUpdateConnection)
    {
        foreach (string[] kv in parameters)
        {
            if (kv.Length == 1 || "".Equals(kv[1]))
            {
                queryParameters.Remove(kv[0]);
                shouldUpdateConnection = true;
            }
            else
            {
                switch (kv[0])
                {
                    case "database":
                        if (!kv[1].Equals(connectionBuilder.Database))
                        {
                            connectionBuilder.Database = kv[1];
                            shouldUpdateConnection = true;
                        }
                        break;
                    case "engine":
                        if (!kv[1].Equals(connectionBuilder.Engine))
                        {
                            connectionBuilder.Engine = kv[1];
                            shouldUpdateConnection = true;
                        }
                        break;
                    case "account_id":
                        _connection.AccountId = kv[1];
                        break;
                    default:
                        if (!kv[1].Equals(queryParameters[kv[0]]))
                        {
                            queryParameters[kv[0]] = kv[1];
                            shouldUpdateConnection = true;
                        }
                        break;
                }
            }
        }
        return shouldUpdateConnection;
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

    public async Task<string> EstablishConnection(bool forceTokenRefresh = false)
    {
        LoginResponse? loginResponse = forceTokenRefresh ? null : await _tokenStorage.GetCachedToken(_id, _secret);
        if (loginResponse == null)
        {
            loginResponse = await Login(_id, _secret, _env);
            await _tokenStorage.CacheToken(loginResponse, _id, _secret);
        }
        _token = loginResponse.Access_token;
        return _token;
    }

    public abstract Task<ConnectionResponse> ConnectAsync(string? engineName, string database, CancellationToken cancellationToken);

    internal abstract void CleanupCache();
}
