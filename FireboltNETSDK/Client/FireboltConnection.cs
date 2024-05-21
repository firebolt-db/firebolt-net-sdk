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
using FireboltDotNetSdk.Utils;
using IsolationLevel = System.Data.IsolationLevel;
using static FireboltDotNetSdk.Client.FireResponse;

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
        private FireboltConnectionState _connectionState;

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
        private int _infraVersion = 0;

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
                    GetAccountIdByNameResponse account = Client.GetAccountIdByNameAsync(Account, CancellationToken.None).GetAwaiter().GetResult();
                    _accountId = account.id;
                    _infraVersion = account.infraVersion;
                }
                else if (_infraVersion == 0)
                {
                    _infraVersion = 1; // older versions of DB does not supply infra version, so we assume 1
                }
                return _accountId;
            }
            set => _accountId = value;
        }

        internal int InfraVersion
        {
            get => _infraVersion;
            set => _infraVersion = value;
        }

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
                ValidateConnection();
                OnSessionEstablished();
                return EngineUrl != null;
            }
            catch (System.Exception)
            {
                Close();
                throw;
            }
        }

        internal void ValidateConnection()
        {
            CreateDbCommand("SELECT 1").ExecuteScalar();
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

        internal void UpdateConnectionSettings(FireboltConnectionStringBuilder builder, CancellationToken cancellationToken)
        {
            _connectionString = builder.ToConnectionString();
            FireboltConnectionSettings settings = builder.BuildSettings();
            _database = settings.Database ?? string.Empty;
            EngineName = settings?.Engine;
            _isSystem = EngineName == null || SYSTEM_ENGINE.Equals(EngineName);
        }

    }
}
