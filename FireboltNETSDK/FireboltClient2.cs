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

namespace FireboltDotNetSdk;
public class FireboltClient2 : FireboltClient
{
    private const string PROTOCOL_VERSION = "2.3";
    private readonly ISet<string> _engineStatusesRunning = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase) { "Running", "ENGINE_STATE_RUNNING" };
    private readonly string _account;
    private static IDictionary<string, string> systemEngineUrlCache = new ConcurrentDictionary<string, string>();
    private static string DB_QUERY = "SELECT * FROM information_schema.{0}s WHERE {0}_name=@Name";

    public FireboltClient2(FireboltConnection connection, string id, string secret, string endpoint, string? env, string account, HttpClient httpClient) : base(connection, id, secret, endpoint, env, "2.0", httpClient)
    {
        _account = account;
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
        string cacheKey = $"{_env}.{_account}";
        string? systemEngineUrl;
        systemEngineUrlCache.TryGetValue(cacheKey, out systemEngineUrl);
        // Connecting to system engine by default
        if (systemEngineUrl == null)
        {
            var result = await GetSystemEngineUrl(_account);
            systemEngineUrl = result.engineUrl;
        }
        if (systemEngineUrl != null)
        {
            systemEngineUrlCache[cacheKey] = systemEngineUrl;
            string[] urlParts = systemEngineUrl.Split('?');
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
        if (!string.IsNullOrEmpty(database))
        {
            await Execute($"USE DATABASE \"{database}\"");
        }
        await Execute($"USE ENGINE \"{engineName}\"");
        return new ConnectionResponse(_connection.EngineUrl, database ?? string.Empty, false);
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
    }

    public override Task<GetAccountIdByNameResponse> GetAccountIdByNameAsync(string account, CancellationToken cancellationToken)
    {
        // For 2.0 this is a no-op as account id is fetched during use engine command
        // Keeping this implementation since connection is independent of 1.0 and 2.0
        return Task.FromResult(new GetAccountIdByNameResponse() { id = null, infraVersion = 2 });
    }
}
