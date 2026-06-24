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

using System.Text;
using FireboltDotNetSdk.Client;
using Microsoft.AspNetCore.WebUtilities;
using Newtonsoft.Json.Linq;
using static FireboltDotNetSdk.Client.FireResponse;

namespace FireboltDotNetSdk;

public class FireboltClientCore : FireboltClient
{
    private const string DiscoveryPath = "/.well-known/firebolt";
    private readonly string _configuredUrl;
    private readonly string _sslMode;
    private readonly IDictionary<string, string> _connectionParameters = new Dictionary<string, string>();
    private string? _resolvedEngineUrl;

    public FireboltClientCore(FireboltConnection connection, string url, string sslMode, HttpClient httpClient)
        : base(connection, string.Empty, string.Empty, url, null, null, httpClient)
    {
        _configuredUrl = url;
        _sslMode = sslMode;
    }

    public override Task<string> EstablishConnection(bool forceTokenRefresh = false)
    {
        _token = string.Empty;
        return Task.FromResult(_token);
    }

    public override async Task<ConnectionResponse> ConnectAsync(string? engineName, string database,
        CancellationToken cancellationToken)
    {
        var resolved = await ResolveEngineEndpoint(cancellationToken);
        _resolvedEngineUrl = resolved.EngineUrl;
        foreach (var parameter in resolved.Parameters)
        {
            _connectionParameters[parameter.Key] = parameter.Value;
        }

        _connection.InfraVersion = 2;
        return new ConnectionResponse(_resolvedEngineUrl, database, false);
    }

    protected override Task<LoginResponse> Login(string id, string secret, string env)
    {
        return Task.FromResult(new LoginResponse(string.Empty, "0", "Bearer"));
    }

    public override Task<GetAccountIdByNameResponse> GetAccountIdByNameAsync(string account,
        CancellationToken cancellationToken)
    {
        return Task.FromResult(new GetAccountIdByNameResponse { id = null, infraVersion = 2 });
    }

    protected override Task<HttpRequestMessage> GetHttpRequest(HttpMethod method, string uri, HttpContent? content,
        bool needsAccessToken)
    {
        return base.GetHttpRequest(method, uri, content, needsAccessToken: false);
    }

    protected override string GetUrl(string engineEndpoint, string? databaseName, string? accountId,
        HashSet<string> setParamList, bool isStreamingRequest)
    {
        var urlBuilder = new UriBuilder(engineEndpoint);
        var queryStr = GetQueryString(string.IsNullOrEmpty(databaseName) ? null : databaseName, accountId,
            isStreamingRequest);
        foreach (var parameter in _connectionParameters)
        {
            AppendQueryParameter(queryStr, parameter.Key, parameter.Value);
        }
        foreach (var parameter in setParamList)
        {
            queryStr.Append(queryStr.Length > 0 ? "&" : string.Empty).Append(parameter);
        }

        urlBuilder.Query = queryStr.ToString();
        return urlBuilder.Uri.ToString();
    }

    internal override void CleanupCache()
    {
        _connectionParameters.Clear();
        _resolvedEngineUrl = null;
    }

    private async Task<ResolvedEndpoint> ResolveEngineEndpoint(CancellationToken cancellationToken)
    {
        if (_resolvedEngineUrl != null)
        {
            return new ResolvedEndpoint(_resolvedEngineUrl, _connectionParameters);
        }

        var configuredUri = BuildConfiguredUri(_configuredUrl, _sslMode);
        var configuredParameters = ExtractQueryParameters(configuredUri);
        var configuredEngineUrl = RemoveQuery(configuredUri);

        var discoveryUri = BuildDiscoveryUri(configuredUri);
        using var response = await _httpClient.GetAsync(discoveryUri, cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            return new ResolvedEndpoint(configuredEngineUrl, configuredParameters);
        }

        var responseBody = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        FireboltDiscoveryEndpoint discoveryEndpoint;
        try
        {
            discoveryEndpoint = FireboltDiscoveryEndpoint.Parse(responseBody);
        }
        catch (System.Exception)
        {
            return new ResolvedEndpoint(configuredEngineUrl, configuredParameters);
        }
        if (discoveryEndpoint.EngineUrl == null)
        {
            return new ResolvedEndpoint(configuredEngineUrl, configuredParameters);
        }

        var discoveredUri = BuildConfiguredUri(discoveryEndpoint.EngineUrl, _sslMode);
        var parameters = ExtractQueryParameters(discoveredUri);
        foreach (var parameter in discoveryEndpoint.Parameters)
        {
            parameters[parameter.Key] = parameter.Value;
        }
        foreach (var parameter in configuredParameters)
        {
            parameters[parameter.Key] = parameter.Value;
        }
        return new ResolvedEndpoint(RemoveQuery(discoveredUri), parameters);
    }

    private static Uri BuildConfiguredUri(string url, string sslMode)
    {
        var hasScheme = Uri.TryCreate(url, UriKind.Absolute, out var uri)
                        && (uri.Scheme == Uri.UriSchemeHttp || uri.Scheme == Uri.UriSchemeHttps);
        if (hasScheme)
        {
            return uri!;
        }

        var scheme = sslMode == "none" ? "http" : "https";
        return new Uri($"{scheme}://{url}", UriKind.Absolute);
    }

    private static Uri BuildDiscoveryUri(Uri configuredUri)
    {
        return new UriBuilder(configuredUri)
        {
            Path = DiscoveryPath,
            Query = string.Empty
        }.Uri;
    }

    private static string RemoveQuery(Uri uri)
    {
        return new UriBuilder(uri)
        {
            Query = string.Empty
        }.Uri.ToString();
    }

    private static IDictionary<string, string> ExtractQueryParameters(Uri uri)
    {
        return QueryHelpers.ParseQuery(uri.Query)
            .Where(parameter => parameter.Value.Count > 0)
            .ToDictionary(parameter => parameter.Key, parameter => parameter.Value[0] ?? string.Empty,
                StringComparer.OrdinalIgnoreCase);
    }

    private static void AppendQueryParameter(StringBuilder query, string key, string value)
    {
        var existingParameters = QueryHelpers.ParseQuery("?" + query);
        if (existingParameters.ContainsKey(key))
        {
            return;
        }

        query.Append(query.Length > 0 ? "&" : string.Empty)
            .Append(Uri.EscapeDataString(key))
            .Append('=')
            .Append(Uri.EscapeDataString(value));
    }

    private readonly struct ResolvedEndpoint
    {
        public ResolvedEndpoint(string engineUrl, IDictionary<string, string> parameters)
        {
            EngineUrl = engineUrl;
            Parameters = parameters;
        }

        public string EngineUrl { get; }
        public IDictionary<string, string> Parameters { get; }
    }

    private readonly struct FireboltDiscoveryEndpoint
    {
        private FireboltDiscoveryEndpoint(string? engineUrl, IDictionary<string, string> parameters)
        {
            EngineUrl = engineUrl;
            Parameters = parameters;
        }

        public string? EngineUrl { get; }
        public IDictionary<string, string> Parameters { get; }

        public static FireboltDiscoveryEndpoint Parse(string body)
        {
            var root = JObject.Parse(body);
            var engineUrl = FindString(root, "engine_url", "engineUrl", "url", "endpoint", "query_url",
                "queryUrl", "queryEndpoint", "engine.url", "engine.endpoint", "endpoints.query",
                "endpoints.sql", "http.url", "http.endpoint");
            if (engineUrl == null && root.SelectToken("engines") is JArray engines)
            {
                engineUrl = engines
                    .Select(engine => FindString(engine, "url", "endpoint", "engine_url", "engineUrl"))
                    .FirstOrDefault(value => value != null);
            }

            var parameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var tokenPath in new[] { "parameters", "params", "query_parameters", "queryParameters" })
            {
                if (root.SelectToken(tokenPath) is JObject parameterObject)
                {
                    foreach (var property in parameterObject.Properties())
                    {
                        parameters[property.Name] = property.Value.ToString();
                    }
                }
            }

            return new FireboltDiscoveryEndpoint(engineUrl, parameters);
        }

        private static string? FindString(JToken root, params string[] paths)
        {
            return paths
                .Select(path => root.SelectToken(path)?.ToString())
                .FirstOrDefault(value => !string.IsNullOrEmpty(value));
        }
    }
}
