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
using System.Data.Common;
using System.Collections.Concurrent;
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Client;
using static FireboltDotNetSdk.Client.FireRequest;
using static FireboltDotNetSdk.Client.FireResponse;
using static FireboltDotNetSdk.Utils.Constant;
using FireboltDotNetSdk.Utils;

namespace FireboltDotNetSdk;
public class FireboltClient2 : FireboltClient
{
    private const string PROTOCOL_VERSION = "2.4";
    private readonly ISet<string> _engineStatusesRunning = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase) { "Running", "ENGINE_STATE_RUNNING" };
    private readonly string _account;
    private readonly string _connectionId;
    private static IDictionary<string, string> systemEngineUrlCache = new ConcurrentDictionary<string, string>();
    private static string DB_QUERY = "SELECT * FROM information_schema.{0}s WHERE {0}_name=@Name";

    public FireboltClient2(FireboltConnection connection, string id, string secret, string endpoint, string? env, string account, HttpClient httpClient) : base(connection, id, secret, endpoint, env, "2.0", httpClient)
    {
        _account = account;
        _connectionId = GenerateConnectionId();
    }

    /// <summary>
    /// Generates a unique connection ID using a GUID.
    /// </summary>
    private static string GenerateConnectionId()
    {
        return Guid.NewGuid().ToString();
    }

    /// <summary>
    /// Checks if connection caching is enabled for this connection.
    /// Available only for Firebolt 2.0 connections. Default is true.
    /// </summary>
    private bool IsConnectionCachingEnabled()
    {
        return _connection.IsCacheConnectionEnabled;
    }

    /// <summary>
    /// Overrides GetHttpRequest to add connection info to User-Agent header.
    /// </summary>
    protected override async Task<HttpRequestMessage> GetHttpRequest(HttpMethod method, string uri, HttpContent? content, bool needsAccessToken)
    {
        var request = await base.GetHttpRequest(method, uri, content, needsAccessToken);
        
        if (!IsConnectionCachingEnabled())
        {
            return request; // Skip adding connection tracking info if caching is disabled
        }
        
        try
        {
            // Add connection info to User-Agent header
            var cacheKey = new CacheKey(_id, _secret, _account);
            var connectionCache = CacheService.Instance.Get(cacheKey.GetValue());
            
            var connectionInfo = new System.Text.StringBuilder($" connId:{_connectionId}");
            
            // If connection is cached and has different connectionId, add cached connection info
            if (connectionCache != null && !_connectionId.Equals(connectionCache.ConnectionId))
            {
                connectionInfo.Append($"; cachedConnId:{connectionCache.ConnectionId}-{connectionCache.CacheSource}");
            }
            
            // Append to existing User-Agent using TryAddWithoutValidation to avoid format validation issues
            var existingUserAgent = _httpClient.DefaultRequestHeaders.UserAgent.ToString();
            request.Headers.Remove("User-Agent");
            request.Headers.TryAddWithoutValidation("User-Agent", existingUserAgent + connectionInfo);
            
            return request;
        }
        catch
        {
            request?.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Establishes connection with JWT token caching for V2.
    /// V2 uses ConnectionCache in addition to legacy TokenStorage.
    /// </summary>
    /// <param name="forceTokenRefresh">Whether to force a new token refresh.</param>
    /// <returns>The access token.</returns>
    public override async Task<string> EstablishConnection(bool forceTokenRefresh = false)
    {
        LoginResponse? loginResponse = null;
        
        if (!IsConnectionCachingEnabled())
        {
            // Caching disabled, use base implementation (legacy TokenStorage only)
            return await base.EstablishConnection(forceTokenRefresh);
        }
        
        // V2 uses ConnectionCache for JWT token caching
        var cacheKey = new CacheKey(_id, _secret, _account);
        var connectionCache = CacheService.Instance.GetOrCreate(cacheKey.GetValue(), _connectionId);
        
        if (!forceTokenRefresh)
        {
            // Try ConnectionCache first
            loginResponse = connectionCache.GetCachedToken();
            
            // Fallback to legacy TokenStorage if ConnectionCache doesn't have it
            if (loginResponse == null)
            {
                loginResponse = await _tokenStorage.GetCachedToken(_id, _secret);
                
                // If found in legacy storage, also cache in ConnectionCache
                if (loginResponse != null)
                {
                    connectionCache.SetCachedToken(loginResponse);
                }
            }
        }
        
        if (loginResponse == null)
        {
            loginResponse = await Login(_id, _secret, _env);
            
            // Set expiry before caching
            loginResponse.Expires_in = (Convert.ToInt32(loginResponse.Expires_in) + Constant.GetCurrentEpoch()).ToString();
            
            // Cache in both ConnectionCache and legacy TokenStorage
            connectionCache.SetCachedToken(loginResponse);
            await _tokenStorage.CacheToken(loginResponse, _id, _secret);
        }
        
        // Set token field and return (consistent with base implementation)
        _token = loginResponse.Access_token;
        return _token;
    }

    /// <summary>
    ///     Authenticates the user with Firebolt.
    /// </summary>
    /// <param name="id"></param>
    /// <param name="sectet"></param>
    /// <returns></returns>
    protected override Task<LoginResponse> Login(string id, string secret, string env)
    {
        var credentials = new ServiceAccountLoginRequest(id, secret);
        return SendAsync<LoginResponse>(HttpMethod.Post, $"https://id.{env}.firebolt.io{AUTH_SERVICE_ACCOUNT_URL}", credentials.GetFormUrlEncodedContent(), false, false, CancellationToken.None);
    }

    /// <summary>
    ///     Fetches system engine URL.
    /// </summary>
    /// <param name="accountName">Name of the account</param>
    /// <returns>Engine URL response</returns>
    public async Task<GetSystemEngineUrlResponse> GetSystemEngineUrl(string accountName)
    {
        return await GetAccountField<GetSystemEngineUrlResponse>("engineUrl", accountName);
    }

    private async Task<T> GetAccountField<T>(string fieldName, string accountName)
    {
        var url = new UriBuilder()
        {
            Scheme = "https",
            Host = _endpoint,
            Path = $"/web/v3/account/{accountName}/{fieldName}"
        }.Uri.ToString();

        try
        {
            return await GetJsonResponseAsync<T>(HttpMethod.Get, url, body: null, requiresAuth: true, CancellationToken.None);
        }
        catch (FireboltException e) when (e.StatusCode == HttpStatusCode.NotFound)
        {
            throw new FireboltException(
                $"Account '{accountName}' does not exist in this organization or is not authorized. " +
                "Please verify the account name and make sure your service account has the correct RBAC permissions and is linked to a user.", e)
            { StatusCode = e.StatusCode };

        }
    }

    /// <summary>
    /// Opens a database connection asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation instruction.</param>
    /// <returns>A <see cref="Task"/> representing asynchronous operation.</returns>
    public override async Task<ConnectionResponse> ConnectAsync(string? engineName, string database, CancellationToken cancellationToken)
    {
        await EstablishConnection();
        
        string? systemEngineUrl = null;
        
        if (IsConnectionCachingEnabled())
        {
            // Get or create the connection cache for this client
            var cacheKey = new CacheKey(_id, _secret, _account);
            var connectionCache = CacheService.Instance.GetOrCreate(cacheKey.GetValue(), _connectionId);
            
            // Try to get system engine URL from ConnectionCache first
            systemEngineUrl = connectionCache.GetSystemEngineUrl();
        }
        
        // Fallback to legacy static cache if not in ConnectionCache
        if (systemEngineUrl == null)
        {
            string legacyCacheKey = $"{_env}.{_account}";
            systemEngineUrlCache.TryGetValue(legacyCacheKey, out systemEngineUrl);
        }
        
        // Fetch from server if not cached anywhere
        if (systemEngineUrl == null)
        {
            var result = await GetSystemEngineUrl(_account);
            systemEngineUrl = result.engineUrl;
        }
        
        if (systemEngineUrl != null)
        {
            // Cache in ConnectionCache if enabled, always cache in legacy cache
            if (IsConnectionCachingEnabled())
            {
                var cacheKey = new CacheKey(_id, _secret, _account);
                var connectionCache = CacheService.Instance.GetOrCreate(cacheKey.GetValue(), _connectionId);
                connectionCache.SetSystemEngineUrl(systemEngineUrl);
            }
            
            var legacyCacheKey = $"{_env}.{_account}";
            systemEngineUrlCache[legacyCacheKey] = systemEngineUrl;
            
            var urlParts = systemEngineUrl.Split('?');
            _connection.EngineUrl = urlParts[0];
            _connection.InfraVersion = 2;
            if (urlParts.Length > 1)
            {
                ProcessParameters(new FireboltConnectionStringBuilder(_connection.ConnectionString), ExtractParameters(urlParts[1]), false);
            }
        }
        if (engineName == null || FireboltConnection.SYSTEM_ENGINE.Equals(engineName))
        {
            return await ConnectToSystemEngine(database);
        }
        // Engine is specified
        return await ConnectToCustomEngine(_connection.InfraVersion, engineName, database);
    }

    private async Task<ConnectionResponse> ConnectToSystemEngine(string database)
    {
        await Execute("select 1"); // needed to get the InfraVersion back
        if (_connection.InfraVersion == 2)
        {
            _protocolVersion = PROTOCOL_VERSION;
        }
        return new ConnectionResponse(_connection.EngineUrl, database, true);
    }


    private async Task<ConnectionResponse> ConnectToCustomEngine(int infraVersion, string engineName, string database)
    {
        switch (infraVersion)
        {
            case 1: return await ConnectToCustomEngineUsingInformationSchema(engineName, database);
            case 2:
                _protocolVersion = PROTOCOL_VERSION;
                return await ConnectToCustomEngineUsingResponseHeaders(engineName, database);
            default: throw new FireboltException($"Unexpected infrastructure version {infraVersion}");
        }
    }

    private async Task<ConnectionResponse> ConnectToCustomEngineUsingInformationSchema(string engineName, string database)
    {
        if (database == string.Empty)
        {
            // If no db provided - try to fetch it
            database = await GetEngineDatabase(engineName) ?? throw new FireboltException($"Engine {engineName} is attached to a database current user can not access");
        }
        string dbTerm = await GetDatabaseTable();
        var hasAccess = await IsDatabaseAccessible(dbTerm, database);
        if (!hasAccess)
        {
            throw new FireboltException($"Database {database} does not exist or current user does not have access to it!");
        }
        var query = @$"SELECT engs.url, engs.attached_to, dbs.{0}_name, status
                    FROM information_schema.engines as engs
                    LEFT JOIN information_schema.{0}s as dbs
                    ON engs.attached_to = dbs.{0}_name
                    WHERE engs.engine_name = @EngineName";
        DbDataReader reader = await Query(string.Format(query, dbTerm), "@EngineName", engineName);
        if (!await reader.ReadAsync())
        {
            throw new FireboltException($"Engine {engineName} not found.");
        }
        if (await reader.IsDBNullAsync(1))
        {
            throw new FireboltException($"Engine {engineName} is not attached to any database");
        }
        if (database != null && reader.GetString(1) != database)
        {
            throw new FireboltException($"Engine {engineName} is not attached to {database}");
        }
        if (!_engineStatusesRunning.Contains(reader.GetString(3)))
        {
            throw new FireboltException($"Engine {engineName} is not running");
        }
        if (await reader.ReadAsync())
        {
            throw new FireboltException($"Unexpected duplicate entries found for {engineName} and database {database}");
        }
        return new ConnectionResponse(reader.GetString(0).Split("?", 2)[0], database ?? string.Empty, false);
    }

    private async Task<ConnectionResponse> ConnectToCustomEngineUsingResponseHeaders(string engineName, string database)
    {
        if (!IsConnectionCachingEnabled())
        {
            // Caching disabled, execute USE statements directly
            if (!string.IsNullOrEmpty(database))
            {
                await Execute($"USE DATABASE \"{database}\"");
            }
            await Execute($"USE ENGINE \"{engineName}\"");
            return new ConnectionResponse(_connection.EngineUrl, database ?? string.Empty, false);
        }
        
        var cacheKey = new CacheKey(_id, _secret, _account);
        var connectionCache = CacheService.Instance.GetOrCreate(cacheKey.GetValue(), _connectionId);

        if (!string.IsNullOrEmpty(database))
        {
            await GetAndSetDatabaseProperties(database, connectionCache, cacheKey);
        }
        await GetAndSetEngineProperties(engineName, connectionCache, cacheKey);
        return new ConnectionResponse(_connection.EngineUrl, database ?? string.Empty, false);
    }

    /// <summary>
    /// Once we verify a database with the backend we will cache its result and would consider the database valid in between connection establishing.
    /// If the database was deleted between the time a connection cached the database and the second connection using the cached value, then this would only
    /// be caught while executing the statement, not during connection time.
    /// </summary>
    /// <param name="databaseName">The name of the database to check</param>
    /// <param name="connectionCache">The connection cache to use</param>
    /// <param name="cacheKey">The cache key for storing results</param>
    private async Task GetAndSetDatabaseProperties(string databaseName, ConnectionCache connectionCache, CacheKey cacheKey)
    {
        // Check the cache first
        if (connectionCache.IsDatabaseValidated(databaseName))
        {
            // Database already validated, no need to execute USE DATABASE
            return;
        }

        // Use lock to ensure thread safety
        lock (connectionCache)
        {
            // Double-check another thread did not already validate it
            if (connectionCache.IsDatabaseValidated(databaseName))
            {
                return;
            }
        }

        // We know for sure it is not validated, so execute the statement
        await Execute($"USE DATABASE \"{databaseName}\"");

        // Mark the database as validated in the cache
        // Note: Response headers from USE DATABASE are automatically processed by ProcessResponseHeaders in FireboltClient
        lock (connectionCache)
        {
            connectionCache.SetDatabaseValidated(databaseName);
        }
    }

    /// <summary>
    /// Once we verify an engine with the backend we will cache its result and would consider the engine valid in between connection establishing.
    /// If the engine was deleted between the time a connection cached the engine and the second connection using the cached value, then this would only
    /// be caught while executing the statement, not during connection time.
    /// </summary>
    /// <param name="engineName">The name of the engine to check</param>
    /// <param name="connectionCache">The connection cache to use</param>
    /// <param name="cacheKey">The cache key for storing results</param>
    private async Task GetAndSetEngineProperties(string engineName, ConnectionCache connectionCache, CacheKey cacheKey)
    {
        // Check the cache first
        var engineOptions = connectionCache.GetEngineOptions(engineName);

        if (engineOptions != null)
        {
            UpdateEngineOptionsOnConnection(engineOptions);
            return;
        }

        // Use lock to ensure thread safety
        lock (connectionCache)
        {
            // Double-check another thread did not already update the engine
            engineOptions = connectionCache.GetEngineOptions(engineName);

            if (engineOptions != null)
            {
                UpdateEngineOptionsOnConnection(engineOptions);
                return;
            }
        }

        await Execute($"USE ENGINE \"{engineName}\"");

        var engineProperties = new List<KeyValuePair<string, string>>
        {
            new("engine", _connection.EngineName ?? engineName)
        };

        lock (connectionCache)
        {
            connectionCache.SetEngineOptions(engineName, new EngineOptions(_connection.EngineUrl ?? string.Empty, engineProperties));
        }
    }

    /// <summary>
    /// Updates the engine options on the connection using cached values.
    /// </summary>
    /// <param name="engineOptions">The cached engine options</param>
    private void UpdateEngineOptionsOnConnection(EngineOptions engineOptions)
    {
        // Set the engine url
        _connection.EngineUrl = engineOptions.EngineUrl;

        // Need to set the values on the connection that were cached on the original use engine call
        var builder = new FireboltConnectionStringBuilder(_connection.ConnectionString);
        foreach (var pair in engineOptions.Parameters)
        {
            switch (pair.Key)
            {
                case "engine":
                    builder.Engine = pair.Value;
                    break;
            }
        }
        _connection.UpdateConnectionSettings(builder, CancellationToken.None);
    }

    private async Task<string?> GetEngineDatabase(string engineName)
    {
        DbDataReader reader = await Query("SELECT attached_to FROM information_schema.engines WHERE engine_name=@EngineName", "@EngineName", engineName);
        return await reader.ReadAsync() ? reader.GetString(0) : null;
    }

    private async Task<string> GetDatabaseTable()
    {
        return await (await Query(string.Format(DB_QUERY, "table"), "@Name", "catalogs")).ReadAsync() ? "catalog" : "database";
    }

    private async Task<bool> IsDatabaseAccessible(string table, string database)
    {
        return await (await Query(string.Format(DB_QUERY, table), "@Name", database)).ReadAsync();
    }

    private async Task<DbDataReader> Query(string query, string paramName, string paramValue)
    {
        var command = CreateCommand(query);
        command.Parameters.Add(new FireboltParameter(paramName, paramValue));
        return await command.ExecuteReaderAsync();
    }

    private async Task<int> Execute(string query)
    {
        return await CreateCommand(query).ExecuteNonQueryAsync();
    }

    private DbCommand CreateCommand(string sql)
    {
        var command = _connection.CreateCommand();
        command.CommandText = sql;
        return command;
    }

    internal override void CleanupCache()
    {
        systemEngineUrlCache.Clear();
        CacheService.Instance.Clear();
    }

    public override Task<GetAccountIdByNameResponse> GetAccountIdByNameAsync(string account, CancellationToken cancellationToken)
    {
        // For 2.0 this is a no-op as account id is fetched during use engine command
        // Keeping this implementation since connection is independent of 1.0 and 2.0
        return Task.FromResult(new GetAccountIdByNameResponse() { id = null, infraVersion = 2 });
    }
}
