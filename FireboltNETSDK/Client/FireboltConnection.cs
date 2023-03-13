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

        public FireboltClient? Client { get; private set; }
        public string? EngineUrl { get; private set; }

        /// <summary>
        /// Gets the name of the database specified in the connection settings.
        /// </summary>
        /// <returns>The name of the database specified in the connection settings. The default value is an empty string.</returns>
        public override string Database => _connectionState.Settings?.Database ?? throw new FireboltException("Database is missing");

        public string Password
        {
            get => _connectionState.Settings?.Password ?? throw new FireboltException("Password parameter is missing in the connection string");
            set => throw new NotImplementedException();
        }

        public string Endpoint
        {
            get => _connectionState.Settings?.Endpoint ?? Constant.BaseUrl;
            set => throw new NotImplementedException();
        }

        public string Account
        {
            get => _connectionState.Settings?.Account ?? string.Empty;
            set => throw new NotImplementedException();
        }

        public string UserName
        {
            get => _connectionState.Settings?.UserName ?? throw new FireboltException("UserName is missing");
            set => throw new NotImplementedException();
        }

        private string? EngineName
        {
            get => _connectionState.Settings?.EngineName;
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
            Client = new FireboltClient(UserName, Password, Endpoint);
            await Client.EstablishConnection();
            if (_connectionState.Settings?.EngineUrl != null)
            {
                EngineUrl = _connectionState.Settings?.EngineUrl;
            }
            else
            {
                EngineUrl = EngineName != null ? GetEngineUrlByEngineName(EngineName, Client) : GetDefaultEngineUrl(Client);
            }
            OnSessionEstablished();
            return EngineUrl != null;
        }


        private string? GetDefaultEngineUrl(FireboltClient client)
        {
            try
            {
                return client
                    .GetEngineUrlByDatabaseName(
                        _connectionState.Settings?.Database ??
                        throw new FireboltException("Missing database parameter"), _connectionState.Settings?.Account)
                    .GetAwaiter().GetResult().Engine_url;
            }
            catch (System.Exception ex)
            {
                throw new FireboltException(
                    $"Cannot get url of default engine url from {_connectionState.Settings?.Database} database", ex);
            }
        }

        private string? GetEngineUrlByEngineName(string engineName, FireboltClient client)
        {
            try
            {
                var engine = client
                    .GetEngineIdByEngineName(engineName, _connectionState.Settings?.Account)
                    .GetAwaiter()
                    .GetResult();
                if (engine.engine_id == null)
                {
                    throw new FireboltException($"Cannot find an engine with name: {engineName}");
                }
                var result = client.GetEngineUrlByEngineId(engine.engine_id.engine_id,
                        engine.engine_id.account_id).GetAwaiter()
                    .GetResult();
                return result.engine?.endpoint;
            }
            catch (System.Exception ex)
            {
                throw new FireboltException(
                    $"Cannot get engine url for {engineName} engine from {_connectionState.Settings?.Database} database", ex);
            }
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
