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
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;
using IsolationLevel = System.Data.IsolationLevel;

namespace FireboltDotNetSdk.Client
{
    /// <summary>
    /// Represents a connection to a Firebolt database. This class cannot be inherited.
    /// </summary>
    public class FireboltConnection : DbConnection
    {
        private readonly FireboltConnectionState _connectionState;

        private const string engineStatusRunning = "Running";

        public FireboltClient? Client { get; private set; }
        public string? EngineUrl { get; private set; }
        private string? _accountId = null;
        private bool _isSystem = true;
        private string _database;

        /// <summary>
        /// Gets the name of the database specified in the connection settings.
        /// </summary>
        /// <returns>The name of the database specified in the connection settings. The default value is an empty string.</returns>
        public override string Database => _database;

        public string ClientSecret
        {
            get => _connectionState.Settings?.ClientSecret ?? throw new FireboltException("ClientSecret parameter is missing in the connection string");
            set => throw new NotImplementedException();
        }

        public string Endpoint
        {
            get => _connectionState.Settings?.Endpoint ?? Constant.DEFAULT_ENDPOINT;
            set => throw new NotImplementedException();
        }

        public string? Env
        {
            get => _connectionState.Settings?.Env ?? Constant.DEFAULT_ENV;
        }

        public string Account
        {
            get => _connectionState.Settings?.Account ?? string.Empty;
            set => throw new NotImplementedException();
        }

        public string ClientId
        {
            get => _connectionState.Settings?.ClientId ?? throw new FireboltException("ClientId is missing");
            set => throw new NotImplementedException();
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

        public override string ServerVersion => throw new NotImplementedException();

        public override string DataSource => throw new NotImplementedException();

        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string ConnectionString { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

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
        public FireboltConnection(FireboltConnectionStringBuilder stringBuilder)
        {
            if (stringBuilder == null)
                throw new ArgumentNullException(nameof(stringBuilder));

            var connectionSettings = stringBuilder.BuildSettings();

            _connectionState = new FireboltConnectionState(ConnectionState.Closed, connectionSettings, 0);
            _database = _connectionState.Settings?.Database ?? string.Empty;
        }

        /// <summary>
        /// Not supported. The database cannot be changed while the connection is open.
        /// </summary>
        /// <exception cref="NotSupportedException">Always throws <see cref="NotSupportedException"/>.</exception>
        public override void ChangeDatabase(string databaseName)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc cref="ChangeDatabase(string)"/>
        public override Task ChangeDatabaseAsync(string databaseName, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Not supported. Transactions are not supported by the Firebolt server.
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
            Client = new FireboltClient(ClientId, ClientSecret, Endpoint, Env);
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
                    throw new FireboltException($"Engine {EngineName} is attached to a dabase current user can not access");
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
            var query = "SELECT attached_to FROM information_schema.engines WHERE engine_name=@EngineName";
            var cursor = CreateCursor();
            cursor.Parameters.AddWithValue("@EngineName", engineName);
            var res = cursor.Execute(query);
            var database = (string?)res?.Data[0][0];
            return database;
        }

        private bool IsDatabaseAccessible(string database)
        {
            var query = "SELECT database_name FROM information_schema.databases " +
                       $"WHERE database_name=@DatabaseName";
            var cursor = CreateCursor();
            cursor.Parameters.AddWithValue("@DatabaseName", database);
            var res = cursor.Execute(query);
            return res?.Data.Count == 1;
        }

        private string? GetEngineUrlByEngineNameAndDb(string engineName, string database)
        {
            var haveAccess = IsDatabaseAccessible(database);
            if (!haveAccess)
            {
                throw new FireboltException($"Database {database} does not exist or current user does not have access to it!");
            }
            return GetEngineUrl(engineName, database);
        }

        private string? GetEngineUrl(string engineName, string database)
        {
            var query = @$"SELECT engs.url, engs.attached_to, dbs.database_name, status 
                        FROM information_schema.engines as engs 
                        LEFT JOIN information_schema.databases as dbs
                        ON engs.attached_to = dbs.database_name 
                        WHERE engs.engine_name = @EngineName";
            var cursor = CreateCursor();
            cursor.Parameters.AddWithValue("@EngineName", engineName);
            var res = cursor.Execute(query);
            if (res?.Data.Count == 0)
            {
                throw new FireboltException($"Engine {engineName} not found.");
            }
            var filteredResult = from row in res?.Data ?? new List<List<object?>>()
                                 where (string?)row[2] == database
                                 select row;
            if (filteredResult.Count() == 0)
            {
                throw new FireboltException($"Engine {engineName} is not attached to {database}.");
            }
            if (filteredResult.Count() > 1)
            {
                throw new FireboltException($"Unexpected duplicate entries found for {engineName} and database ${database}");
            }
            var resultRow = filteredResult.First();
            if ((string?)resultRow[3] != engineStatusRunning)
            {
                throw new FireboltException($"Engine {engineName} is not running");
            }
            return (string?)resultRow[0];
        }

        private void CheckClient()
        {
            if (Client is null)
                throw new NullReferenceException(
                    "Client is not initialised to perform the operation. Make sure the connection is open.");
        }


        /// <summary>
        /// Creates and returns a <see cref="FireboltCommand"/> object associated with the connection.
        /// </summary>
        /// <returns>A <see cref="FireboltCommand"/> object.</returns>
        public FireboltCommand CreateCursor()
        {
            return new FireboltCommand(this);
        }

        /// <summary>
        /// Creates and returns a <see cref="FireboltCommand"/> object associated with the connection.
        /// </summary>
        /// <param name="commandText">The text for a new command.</param>
        /// <returns>A <see cref="FireboltCommand"/> object.</returns>
        public FireboltCommand CreateCursor(string commandText)
        {
            return new FireboltCommand(this) { CommandText = commandText };
        }

        /// <inheritdoc cref="CreateCursor()"/>
        protected override DbCommand CreateDbCommand()
        {
            return CreateCursor();
        }

        public void OnSessionEstablished()
        {
            _connectionState.State = ConnectionState.Open;
        }

        /// <summary>
        /// Closes the connection to the database.
        /// </summary>
        public override void Close()
        {
            _connectionState.State = ConnectionState.Closed;
            Client = null;
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
            throw new NotImplementedException();
        }
    }
}
