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

using System.Data;
using System.Data.Common;
using System.Transactions;
using System.Runtime.CompilerServices;
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;
using IsolationLevel = System.Data.IsolationLevel;


[assembly: InternalsVisibleTo("FireboltDotNetSdk.Tests")]
namespace FireboltDotNetSdk.Client
{
    /// <summary>
    /// Represents a connection to a Firebolt database. This class cannot be inherited.
    /// </summary>
    public class FireboltConnection : DbConnection
    {
        private FireboltConnectionState _connectionState;

        private const string engineStatusRunning = "Running";

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
        public readonly HashSet<string> SetParamList = new();

        /// <summary>
        /// Gets the name of the database specified in the connection settings.
        /// </summary>
        /// <returns>The name of the database specified in the connection settings. The default value is an empty string.</returns>
        public override string Database => _database;

        public string ClientSecret
        {
            get => _connectionState.Settings?.ClientSecret ?? throw new FireboltException("ClientSecret parameter is missing in the connection string");
        }

        public string Endpoint
        {
            get => _connectionState.Settings?.Endpoint ?? Constant.DEFAULT_ENDPOINT;
        }

        public string? Env
        {
            get => _connectionState.Settings?.Env ?? Constant.DEFAULT_ENV;
        }

        public string Account
        {
            get => _connectionState.Settings?.Account ?? string.Empty;
        }

        public string ClientId
        {
            get => _connectionState.Settings?.ClientId ?? throw new FireboltException("ClientId is missing");
        }

        private string? EngineName
        {
            get => _connectionState.Settings?.Engine;
        }

        public string? AccountId
        {
            get
            {
                if (_accountId == null && Account != null && _isSystem)
                {
                    _accountId = Client?.GetAccountIdByNameAsync(Account, CancellationToken.None).GetAwaiter().GetResult().id;
                }
                return _isSystem ? _accountId : null;
            }
        }


        /// <summary>
        /// Gets the state of the connection.
        /// </summary>
        /// <returns>The state of the connection.</returns>
        public override ConnectionState State => _connectionState.State;

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

        public override string DataSource => _database;

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
                if (connectionSettings.Database == Database
                    && connectionSettings.Endpoint == Endpoint && connectionSettings.Env == Env
                    && connectionSettings.Account == Account
                    && connectionSettings.ClientId == ClientId && connectionSettings.ClientSecret == ClientSecret)
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
                if (isOpen)
                {
                    Open();
                }
            }
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

            var connectionSettings = stringBuilder.BuildSettings();

            _connectionState = new FireboltConnectionState(ConnectionState.Closed, connectionSettings, 0);
            _database = _connectionState.Settings?.Database ?? string.Empty;
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
            if (ChangeDatabaseImpl(databaseName))
            {
                return OpenAsync();
            }
            return Task.CompletedTask;
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
            if (_fireboltClient == null)
            {
                _fireboltClient = new FireboltClient(ClientId, ClientSecret, Endpoint, Env, HttpClientSingleton.GetInstance());
            }
            await Client.EstablishConnection();
            // Connecting to system engine by default
            var result = await Client.GetSystemEngineUrl(Account);
            EngineUrl = result.engineUrl;
            // Specific engine and database specified
            if (EngineName != null && Database != string.Empty)
            {
                EngineUrl = GetEngineUrlByEngineNameAndDb(EngineName, Database);
                _isSystem = false;
            }
            else if (EngineName != null)
            {
                // If no db provided - try to fetch it
                var database = GetEngineDatabase(EngineName);
                if (database == null)
                {
                    throw new FireboltException($"Engine {EngineName} is attached to a database current user can not access");
                }
                EngineUrl = GetEngineUrlByEngineNameAndDb(EngineName, database);
                _database = database;
                _isSystem = false;
            }
            OnSessionEstablished();
            return EngineUrl != null;
        }

        private string? GetEngineDatabase(string engineName)
        {
            return (string?)GetEngineData(engineName, "attached_to")?[0];
        }
        private List<object?>? GetEngineData(string engineName, params string[] fields)
        {
            var query = $"SELECT {String.Join(",", fields)} FROM information_schema.engines WHERE engine_name=@EngineName";
            IDictionary<string, object?> parameters = new Dictionary<string, object?> { { "@EngineName", engineName } };
            return getOneLine(query, parameters);
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

        private bool IsDatabaseAccessible(string database)
        {
            var query = "SELECT database_name FROM information_schema.databases WHERE database_name=@DatabaseName";
            IDictionary<string, object?> parameters = new Dictionary<string, object?> { { "@DatabaseName", database } };
            return getLines(query, parameters)?.Count == 1;
        }

        private string? GetEngineUrlByEngineNameAndDb(string engineName, string database)
        {
            var haveAccess = IsDatabaseAccessible(database);
            if (!haveAccess)
            {
                Close();
                throw new FireboltException($"Database {database} does not exist or current user does not have access to it!");
            }
            return GetEngineUrl(engineName, database);
        }

        private string? GetEngineUrl(string engineName, string? database)
        {
            var query = @$"SELECT engs.url, engs.attached_to, dbs.database_name, status 
                        FROM information_schema.engines as engs 
                        LEFT JOIN information_schema.databases as dbs
                        ON engs.attached_to = dbs.database_name 
                        WHERE engs.engine_name = @EngineName";
            var command = CreateCommand();
            command.CommandText = query;
            command.Parameters.Add(new FireboltParameter("@EngineName", engineName));

            DbDataReader reader = command.ExecuteReader();
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

        /// <inheritdoc cref="CreateDbCommand()"/>
        protected override DbCommand CreateDbCommand()
        {
            return new FireboltCommand(this, null, new FireboltParameterCollection());
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

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            return new FireboltTransaction(this);
        }

        private string EditConnectionString(string orig, string name, string value)
        {
            string newKeyValue = $"{name}={value}";
            string[] elements = orig.Split(';');
            bool append = true;
            for (int i = 0; i < elements.Length; i++)
            {
                string[] kv = elements[i].Split('=');
                if (kv[0] == name)
                {
                    if (kv[1] != value)
                    {
                        elements[i] = newKeyValue;
                        append = false;
                    }
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
    }
}
