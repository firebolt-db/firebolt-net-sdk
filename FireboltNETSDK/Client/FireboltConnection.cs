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

using System.Data;
using System.Data.Common;
using System.Transactions;
using System.Runtime.CompilerServices;
using System.Collections.Concurrent;
using static System.Environment;
using FireboltDotNetSdk.Utils;
using IsolationLevel = System.Data.IsolationLevel;
using static FireboltDotNetSdk.Client.FireResponse;
using FireboltDotNetSdk.Exception;

[assembly: InternalsVisibleTo("FireboltDotNetSdk.Tests")]
[assembly: InternalsVisibleTo("FireboltDotNetSdk")]
namespace FireboltDotNetSdk.Client
{
    /// <summary>
    /// Represents a connection to a Firebolt database. This class cannot be inherited.
    /// </summary>
    public class FireboltConnection : DbConnection
    {
        internal readonly static string SYSTEM_ENGINE = "system";
        private readonly FireboltConnectionState _connectionState;

        public FireboltClient Client
        {
            get
            {
                return _fireboltClient!;
            }
            internal set => _fireboltClient = value;
        }
        public string? EngineUrl { get; internal set; }
        private string? _accountId = null;
        private bool _isSystem = true;
        private string _database;
        private string _connectionString;
        private string? _serverVersion;
        private FireboltClient? _fireboltClient;
        public HashSet<string> SetParamList { get; private set; } = new HashSet<string>();
        private static IDictionary<string, GetAccountIdByNameResponse> accountCache = new ConcurrentDictionary<string, GetAccountIdByNameResponse>();

        /// <summary>
        /// Gets the name of the database specified in the connection settings.
        /// </summary>
        /// <returns>The name of the database specified in the connection settings. The default value is an empty string.</returns>
        public override string Database { get => _database; }

        /// <summary>
        /// Either user name or client ID (or any other identifier of person/software that connects to Firebolt)
        /// </summary>
        /// <returns>The principal (user name, client ID etc)</returns>
        public string Principal
        {
            get => _connectionState.Settings.Principal;
        }

        /// <summary>
        /// Either password or client secret (or any other secret sequence that ensures security)
        /// </summary>
        /// <returns>The secret (password, client secret etc)</returns>
        public string Secret
        {
            get => _connectionState.Settings.Secret;
        }

        public string Endpoint
        {
            get => _connectionState.Settings.Endpoint ?? Constant.DEFAULT_ENDPOINT;
        }

        public string Env
        {
            get => _connectionState.Settings.Env ?? Constant.DEFAULT_ENV;
        }

        public string Account
        {
            get => _connectionState.Settings?.Account ?? string.Empty;
        }

        internal TokenStorageType TokenStorageType
        {
            get => _connectionState.Settings?.TokenStorageType ?? TokenStorageType.Memory;
        }

        internal PreparedStatementParamStyleType PreparedStatementParamStyle
        {
            get => _connectionState.Settings?.PreparedStatementParamStyle ?? PreparedStatementParamStyleType.Native;
        }

        internal string? EngineName
        {
            get;
            set;
        }

        internal bool IsSystem
        {
            get => _isSystem;
        }

        public string? AccountId
        {
            get
            {
                if (_accountId == null && Account != null && _isSystem)
                {
                    string cacheKey = $"{Env}.{Account}";
                    GetAccountIdByNameResponse? account = accountCache.ContainsKey(cacheKey) ? accountCache[cacheKey] : null;
                    if (account == null)
                    {
                        account = Client.GetAccountIdByNameAsync(Account, CancellationToken.None).GetAwaiter().GetResult();
                        accountCache[cacheKey] = account;
                    }
                    _accountId = account.id;
                    InfraVersion = account.infraVersion;
                }
                else if (InfraVersion == 0)
                {
                    InfraVersion = 1; // older versions of DB does not supply infra version, so we assume 1
                }
                return _accountId;
            }
            set => _accountId = value;
        }

        internal int InfraVersion { get; set; } = 0;

        /// <summary>
        /// Gets the state of the connection.
        /// </summary>
        /// <returns>The state of the connection.</returns>
        public override ConnectionState State => _connectionState.State;

        /// <summary>
        /// Gets database version.
        /// </summary>
        /// <returns>The version of the database.</returns>
        public override string ServerVersion
        {
            get
            {
                if (_serverVersion == null)
                {
                    _serverVersion = (string?)getOneLine("SELECT VERSION()")?[0] ?? string.Empty;
                }
                return _serverVersion;
            }
        }

        /// <summary>
        /// Gets the name of the database to which to connect.
        /// </summary>
        /// <returns>The name of the database to which to connect.</returns>
        public override string DataSource => _database;

        /// <summary>
        /// Gets provider factory
        /// </summary>
        /// <returns>The implementation of  DbProviderFactory that can be used for creation of the other Firebolt related entities</returns>
        protected override DbProviderFactory DbProviderFactory
        {
            get => FireboltClientFactory.Instance;
        }

        /// <summary>
        /// Connection string that holds all connection parameters as a semicolon separated key-value pairs.
        /// </summary>
        /// <returns>The connection staring</returns>
        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string ConnectionString
        {
            get => _connectionString;
            set
            {
                if (value == null)
                {
                    throw new ArgumentNullException(nameof(value));
                }
                if (value == _connectionString)
                {
                    return;
                }
                var connectionSettings = new FireboltConnectionStringBuilder(value).BuildSettings();
                if (connectionSettings.Database == Database && connectionSettings.Engine == EngineName
                    && connectionSettings.Endpoint == Endpoint && connectionSettings.Env == Env
                    && connectionSettings.Account == Account
                    && connectionSettings.Principal == Principal && connectionSettings.Secret == Secret
                    )
                {
                    _connectionString = value;
                    return;
                }
                bool isOpen = Client != null;
                if (isOpen)
                {
                    Close();
                }

                _connectionString = value;
                if (connectionSettings.Account != Account)
                {
                    _accountId = null;
                }
                _connectionState.Settings = connectionSettings;
                _database = _connectionState.Settings?.Database ?? string.Empty;
                EngineName = _connectionState.Settings?.Engine;
                if (isOpen)
                {
                    Open();
                }
            }
        }

        internal FireboltConnection()
        {
            _connectionState = new FireboltConnectionState(ConnectionState.Closed, new FireboltConnectionSettings(), 0);
            _database = string.Empty;
            _connectionString = string.Empty;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="FireBoltConnection"/> with the settings.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public FireboltConnection(string connectionString)
            : this(new FireboltConnectionStringBuilder(connectionString))
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="FireboltConnection"/> with the settings.
        /// </summary>
        /// <param name="stringBuilder">The connection string builder which will be used for building the connection settings.</param>
        private FireboltConnection(FireboltConnectionStringBuilder stringBuilder)
        {
            if (stringBuilder == null)
                throw new ArgumentNullException(nameof(stringBuilder));

            FireboltConnectionSettings connectionSettings = stringBuilder.BuildSettings();

            _connectionState = new FireboltConnectionState(ConnectionState.Closed, connectionSettings, 0);
            _database = _connectionState.Settings?.Database ?? string.Empty;
            EngineName = _connectionState.Settings?.Engine;
            _connectionString = stringBuilder.ConnectionString;
        }

        /// <summary>
        /// Changes the current database for an open connection. If connection was open, re-opens it. 
        /// </summary>
        /// <exception cref="NotSupportedException">Always throws <see cref="NotSupportedException"/>.</exception>
        public override void ChangeDatabase(string databaseName)
        {
            if (ChangeDatabaseImpl(databaseName))
            {
                Open();
            }
        }

        /// <inheritdoc cref="ChangeDatabase(string)"/>
        public override Task ChangeDatabaseAsync(string databaseName, CancellationToken cancellationToken = default)
        {
            return ChangeDatabaseImpl(databaseName) ? OpenAsync(cancellationToken) : Task.CompletedTask;
        }

        /// <summary>
        /// Not supported. Transactions are not supported by the Firebolt server but we ignore attempts to use them.
        /// </summary>
        /// <exception cref="NotSupportedException">Always throws <see cref="NotSupportedException"/>.</exception>
        public override void EnlistTransaction(Transaction? transaction)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Not supported. Schema information is not implemented.
        /// </summary>
        /// <exception cref="NotImplementedException">Always throws <see cref="NotImplementedException"/>.</exception>
        public override DataTable GetSchema()
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc cref="GetSchema()"/>
        public override DataTable GetSchema(string collectionName)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc cref="GetSchema()"/>
        public override DataTable GetSchema(string collectionName, string?[] restrictionValues)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Opens a database connection asynchronously.
        /// </summary>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A <see cref="Task"/> representing asynchronous operation.</returns>
        public override async Task<bool> OpenAsync(CancellationToken cancellationToken)
        {
            try
            {
                if (_fireboltClient == null)
                {
                    _fireboltClient = CreateClient();
                }
                ConnectionResponse response = await _fireboltClient.ConnectAsync(EngineName!, Database, cancellationToken);
                EngineUrl = response.EngineUrl;
                _isSystem = response.IsSystem;
                _database = response.Database;
                OnSessionEstablished();
                return EngineUrl != null;
            }
            catch (System.Exception)
            {
                await CloseAsync();
                throw;
            }
        }

        internal async Task ValidateConnection(CancellationToken? cancellationToken = null)
        {
            await CreateDbCommand("SELECT 1").ExecuteScalarAsync(cancellationToken ?? CancellationToken.None);
        }

        private FireboltClient CreateClient()
        {
            var builder = new FireboltConnectionStringBuilder(_connectionString);
            switch (builder.Version)
            {
                case 1: return new FireboltClient1(this, Principal, Secret, Endpoint, Env, Account, HttpClientSingleton.GetInstance());
                case 2: return new FireboltClient2(this, Principal, Secret, Endpoint, Env, Account, HttpClientSingleton.GetInstance());
                default: throw new NotSupportedException("Unsupported DB version");
            }
        }

        private List<object?>? getOneLine(string query, IDictionary<string, object?>? parameters = null)
        {
            return getLines(query, parameters)?[0];
        }

        private List<List<object?>>? getLines(string query, IDictionary<string, object?>? parameters = null)
        {
            var command = CreateCommand();
            if (parameters != null)
            {
                foreach (var parameter in parameters)
                {
                    command.Parameters.Add(new FireboltParameter(parameter.Key, parameter.Value));
                }
            }
            return ((FireboltCommand)command).Execute(query)?.Data;
        }

        /// <inheritdoc cref="CreateDbCommand()"/>
        protected override DbCommand CreateDbCommand()
        {
            return CreateDbCommand(null);
        }

        private DbCommand CreateDbCommand(string? command)
        {
            return new FireboltCommand(this, command, new FireboltParameterCollection());
        }

        internal void OnSessionEstablished()
        {
            _connectionState.State = ConnectionState.Open;
        }

        /// <summary>
        /// Closes the connection to the database.
        /// </summary>
        public override void Close()
        {
            _connectionState.State = ConnectionState.Closed;
            _fireboltClient = null;
            _isSystem = true;
        }

        /// <summary>
        /// Opens a database connection.
        /// </summary>
        public override void Open()
        {
            OpenAsync().GetAwaiter().GetResult();
        }

        /// <summary>
        /// Simulates starting transaction. Due to transactions are not supported this method has not effect. 
        /// </summary>
        /// <returns>Simulated implementaion of <see cref="DbTransaction"/>.</returns>
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            return new FireboltTransaction(this);
        }

        private static string EditConnectionString(string orig, string name, string value)
        {
            string newKeyValue = $"{name}={value}";
            string[] elements = orig.Split(';');
            bool append = true;
            for (int i = 0; i < elements.Length; i++)
            {
                string[] kv = elements[i].Split('=');
                if (kv[0] == name && kv[1] != value)
                {
                    elements[i] = newKeyValue;
                    append = false;
                }
            }
            return append ? $"{orig};{newKeyValue}" : string.Join(';', elements);

        }
        private bool ChangeDatabaseImpl(string databaseName)
        {
            if (databaseName == _database)
            {
                return false;
            }
            bool isOpen = Client != null;
            if (isOpen)
            {
                Close();
            }
            _database = databaseName;
            _connectionString = EditConnectionString(_connectionString, "database", databaseName);
            _connectionState.Settings = new FireboltConnectionStringBuilder(_connectionString).BuildSettings();
            return isOpen;
        }

        internal void UpdateConnectionSettings(FireboltConnectionStringBuilder builder, CancellationToken cancellationToken)
        {
            _connectionString = builder.ToConnectionString();
            FireboltConnectionSettings settings = builder.BuildSettings();
            _database = settings.Database ?? string.Empty;
            EngineName = settings.Engine;
            _isSystem = EngineName == null || SYSTEM_ENGINE.Equals(EngineName);
        }

        internal static void CleanupCache()
        {
            accountCache.Clear();
        }

        // Async query status constants
        private const string QueryStatusRunning = "RUNNING";
        private const string QueryStatusEndedSuccessfully = "ENDED_SUCCESSFULLY";


        /// <summary>
        /// Checks if an async query is still running.
        /// </summary>
        /// <param name="token">The token of the async query.</param>
        /// <returns>True if the query is still running, false otherwise.</returns>
        public bool IsServerSideAsyncQueryRunning(string token)
        {
            return IsServerSideAsyncQueryRunningAsync(token).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Checks if an async query is still running asynchronously.
        /// </summary>
        /// <param name="token">The token of the async query.</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A task representing the asynchronous operation with a boolean indicating if the query is still running.</returns>
        public async Task<bool> IsServerSideAsyncQueryRunningAsync(string token, CancellationToken cancellationToken = default)
        {
            var info = await GetAsyncQueryInfoAsync(token, cancellationToken);
            return info.TryGetValue("status", out string? status) && status == QueryStatusRunning;
        }

        /// <summary>
        /// Checks if an async query has completed successfully.
        /// </summary>
        /// <param name="token">The token of the async query.</param>
        /// <returns>True if the query completed successfully, false if it failed, null if it's still running.</returns>
        public bool? IsServerSideAsyncQuerySuccessful(string token)
        {
            return IsServerSideAsyncQuerySuccessfulAsync(token).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Checks if an async query has completed successfully asynchronously.
        /// </summary>
        /// <param name="token">The token of the async query.</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A task representing the asynchronous operation with a boolean indicating if the query completed successfully, 
        /// or null if it's still running.</returns>
        public async Task<bool?> IsServerSideAsyncQuerySuccessfulAsync(string token, CancellationToken cancellationToken = default)
        {
            var info = await GetAsyncQueryInfoAsync(token, cancellationToken);
            info.TryGetValue("status", out string? status);

            // If the query is still running, return null (undefined)
            if (status == QueryStatusRunning)
            {
                return null;
            }

            // Return true if the query completed successfully
            return status == QueryStatusEndedSuccessfully;
        }

        /// <summary>
        /// Gets the status of an async query asynchronously.
        /// </summary>
        /// <param name="token">The token of the async query.</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A task representing the asynchronous operation with a dictionary containing status information for the async query.</returns>
        private async Task<Dictionary<string, string>> GetAsyncQueryInfoAsync(string token, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(token))
            {
                throw new ArgumentNullException(nameof(token), "Token cannot be null or empty");
            }
            DbCommand command = CreateDbCommand("CALL fb_GetAsyncStatus(@token)");
            var tokenParam = command.CreateParameter();
            tokenParam.ParameterName = "@token";
            tokenParam.Value = token;
            command.Parameters.Add(tokenParam);

            using (DbDataReader reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                if (!reader.HasRows || !await reader.ReadAsync(cancellationToken))
                {
                    throw new FireboltException("Invalid response format: missing data");
                }

                var result = new Dictionary<string, string>();

                // Map all fields from the result to the dictionary
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    string fieldName = reader.GetName(i);
                    string value = await reader.IsDBNullAsync(i, cancellationToken) ? string.Empty : reader.GetString(i);
                    result[fieldName] = value;
                }

                return result;
            }
        }

        /// <summary>
        /// Stops an async query execution.
        /// </summary>
        /// <param name="token">The token of the async query.</param>
        /// <returns>True if the query was successfully stopped, false otherwise.</returns>
        public bool CancelServerSideAsyncQuery(string token)
        {
            return CancelServerSideAsyncQueryAsync(token).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Stops an async query execution asynchronously.
        /// </summary>
        /// <param name="token">The token of the async query.</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A task representing the asynchronous operation with a boolean indicating if the query was successfully stopped.</returns>
        public async Task<bool> CancelServerSideAsyncQueryAsync(string token, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(token))
            {
                throw new ArgumentNullException(nameof(token), "Token cannot be null or empty");
            }

            // Get the query status to extract the query_id
            var statusInfo = await GetAsyncQueryInfoAsync(token, cancellationToken);

            // Get the query_id - it's needed for cancellation
            if (!statusInfo.TryGetValue("query_id", out string? queryId) || string.IsNullOrEmpty(queryId))
            {
                throw new FireboltException("Could not find query_id for the running query");
            }

            DbCommand command = CreateDbCommand("CANCEL QUERY WHERE query_id = @queryId");
            var queryIdParam = command.CreateParameter();
            queryIdParam.ParameterName = "@queryId";
            queryIdParam.Value = queryId;
            command.Parameters.Add(queryIdParam);


            await command.ExecuteNonQueryAsync(cancellationToken);

            return true;
        }
    }
}
