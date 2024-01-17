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
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Client;
using static FireboltDotNetSdk.Client.FireRequest;
using static FireboltDotNetSdk.Client.FireResponse;
using static FireboltDotNetSdk.Utils.Constant;

namespace FireboltDotNetSdk;
public class FireboltClient2 : FireboltClient
{
    private const string engineStatusRunning = "Running";
    private readonly string _account;
    public FireboltClient2(FireboltConnection connection, string id, string secret, string endpoint, string? env, string account, HttpMessageInvoker httpClient) : base(connection, id, secret, endpoint, env, "2.0", httpClient)
    {
        _account = account;
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
    public override async Task<GetAccountIdByNameResponse> GetAccountIdByNameAsync(string accountName, CancellationToken cancellationToken)
    {
        return await GetAccountField<GetAccountIdByNameResponse>("resolve", accountName);
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
        return SendAsync<LoginResponse>(HttpMethod.Post, $"https://id.{env}.firebolt.io{AUTH_SERVICE_ACCOUNT_URL}", credentials.GetFormUrlEncodedContent(), false, CancellationToken.None, false);
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
        // Connecting to system engine by default
        var result = await GetSystemEngineUrl(_account);
        _connection.EngineUrl = result.engineUrl;
        if (engineName == null)
        {
            return new ConnectionResponse(result.engineUrl!, database, true);
        }
        // Engine is specified
        if (database == string.Empty)
        {
            // If no db provided - try to fetch it
            database = GetEngineDatabase(engineName) ?? throw new FireboltException($"Engine {engineName} is attached to a database current user can not access");
        }
        return new ConnectionResponse(GetEngineUrlByEngineNameAndDb(engineName, database), database, false);
    }

    private string GetEngineUrlByEngineNameAndDb(string engineName, string database)
    {
        var hasAccess = IsDatabaseAccessible(database);
        if (!hasAccess)
        {
            throw new FireboltException($"Database {database} does not exist or current user does not have access to it!");
        }
        return GetEngineUrl(engineName, database);
    }

    private string GetEngineUrl(string engineName, string? database)
    {
        var query = @$"SELECT engs.url, engs.attached_to, dbs.database_name, status 
                    FROM information_schema.engines as engs 
                    LEFT JOIN information_schema.databases as dbs
                    ON engs.attached_to = dbs.database_name 
                    WHERE engs.engine_name = @EngineName";
        DbDataReader reader = Query(query, "@EngineName", engineName);
        if (!reader.Read())
        {
            throw new FireboltException($"Engine {engineName} not found.");
        }
        if (reader.IsDBNull(1))
        {
            throw new FireboltException($"Engine {engineName} is not attached to any database");
        }
        if (database != null && reader.GetString(1) != database)
        {
            throw new FireboltException($"Engine {engineName} is not attached to {database}");
        }
        if (!engineStatusRunning.Equals(reader.GetString(3), StringComparison.OrdinalIgnoreCase))
        {
            throw new FireboltException($"Engine {engineName} is not running");
        }
        if (reader.Read())
        {
            throw new FireboltException($"Unexpected duplicate entries found for {engineName} and database {database}");
        }
        return reader.GetString(0);
    }

    private string? GetEngineDatabase(string engineName)
    {
        DbDataReader reader = Query("SELECT attached_to FROM information_schema.engines WHERE engine_name=@EngineName", "@EngineName", engineName);
        return reader.Read() ? reader.GetString(0) : null;
    }

    private bool IsDatabaseAccessible(string database)
    {
        return Query("SELECT database_name FROM information_schema.databases WHERE database_name=@DatabaseName", "@DatabaseName", database).Read();
    }

    private DbDataReader Query(string query, string paramName, string paramValue)
    {
        var command = _connection.CreateCommand();
        command.CommandText = query;
        command.Parameters.Add(new FireboltParameter(paramName, paramValue));
        return command.ExecuteReader();
    }
}
