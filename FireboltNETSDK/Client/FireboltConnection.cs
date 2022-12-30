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
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;
using static FireboltDotNetSdk.Client.FireRequest;
using static FireboltDotNetSdk.Client.FireResponse;
using IsolationLevel = System.Data.IsolationLevel;

namespace FireboltDotNetSdk.Client
{
    /// <summary>
    /// Represents a connection to a Firebolt database. This class cannot be inherited.
    /// </summary>
    public class FireboltConnection : DbConnection
    {
        public readonly FireboltClient Client;
        private readonly FireboltConnectionState _connectionState;

        public GetEngineUrlByDatabaseNameResponse? DefaultEngine;
        public GetEngineUrlByEngineNameResponse? Engine;

        /// <summary>
        ///     Initializes a new instance of <see cref="FireBoltConnection" /> with the settings.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public FireboltConnection(string connectionString)
            : this(new FireboltConnectionStringBuilder(connectionString))
        {
        }

        /// <summary>
        ///     Initializes a new instance of <see cref="FireBoltConnection" /> with the settings.
        /// </summary>
        /// <param name="stringBuilder">The connection string builder which will be used for building the connection settings.</param>
        public FireboltConnection(FireboltConnectionStringBuilder stringBuilder)
        {
            if (stringBuilder == null)
                throw new ArgumentNullException(nameof(stringBuilder));

            var connectionSettings = stringBuilder.BuildSettings();

            _connectionState = new FireboltConnectionState(ConnectionState.Closed, connectionSettings, 0);
            Client = FireboltClient.GetInstance();
        }

        private Token? LoginToken { get; set; }

        /// <summary>
        /// Gets the name of the database specified in the connection settings.
        /// </summary>
        /// <returns>The name of the database specified in the connection settings. The default value is an empty string.</returns>
        public override string Database => _connectionState.Settings?.Database ?? throw new FireboltException("Database is missing");

        public string Password
        {
            get => _connectionState.Settings?.Password ?? throw new FireboltException("Password is missing");
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


        /// <summary>
        /// Gets the state of the connection.
        /// </summary>
        /// <returns>The state of the connection.</returns>
        public override ConnectionState State => _connectionState.State;

        public override string ServerVersion => throw new NotImplementedException();

        public override string DataSource => throw new NotImplementedException();

        public override string ConnectionString { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

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
            try
            {
                LoginResponse token;
                var storedToken = await TokenSecureStorage.GetCachedToken(UserName, Password);
                if (storedToken != null)
                {
                    token = new LoginResponse
                    {
                        Access_token = storedToken.token,
                        Expires_in = storedToken.expiration.ToString()
                    };
                }
                else
                {
                    var credentials = new LoginRequest
                    {
                        Password = Password,
                        Username = UserName
                    };
                    token = await Client.Login(credentials, Endpoint);
                    await TokenSecureStorage.CacheToken(token, UserName, Password);
                }

                LoginToken = new Token(token.Access_token, token.Refresh_token, token.Expires_in);

                DefaultEngine = SetDefaultEngine(null);
                OnSessionEstablished();
                if (DefaultEngine != null) return true;
            }
            catch (FireboltException ex)
            {
                throw new FireboltException(ex.Message);
            }

            return false;
        }

        /// <summary>
        ///     Sets the token used for authentication.
        /// </summary>
        /// <param name="token"></param>
        internal string GetAccessToken()
        {
            return LoginToken?.AccessToken;
        }

        public GetEngineUrlByDatabaseNameResponse? SetDefaultEngine(string? engineUrl)
        {
            try
            {
                DefaultEngine = Client
                    .GetEngineUrlByDatabaseName(
                        _connectionState.Settings?.Database ??
                        throw new FireboltException("Missing database parameter"), _connectionState.Settings?.Account,
                        Endpoint, GetAccessToken())
                    .GetAwaiter().GetResult();
                return DefaultEngine;
            }

            catch (System.Exception ex)
            {
                if (ex.Message.Contains("404")) return null;

                throw new FireboltException(
                    $"Cannot get engine: {engineUrl} from {_connectionState.Settings?.Database} database");
            }
        }

        public GetEngineUrlByEngineNameResponse SetEngine(string? engineUrl)
        {
            // try
            // {
            var enginevalue = Client
                .GetEngineUrlByEngineName(engineUrl, _connectionState.Settings?.Account, Endpoint, GetAccessToken())
                .GetAwaiter()
                .GetResult();
            var result = Client.GetEngineUrlByEngineId(enginevalue.engine_id.engine_id,
                    enginevalue.engine_id.account_id, Endpoint, GetAccessToken()).GetAwaiter()
                .GetResult();
            Engine = result;
            return result;
            // }
            // catch (System.Exception ex)
            // {
            //     if (ex.Message.Contains("404")) return null;
            //
            //     throw new FireboltException(
            //         $"Cannot get engine: {engineUrl} from {_connectionState.Settings?.Database} database: {ex.Message}");
            // }
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
            LoginToken = null;
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

        public class Token
        {
            public Token(string? accessToken, string? refreshToken, string? tokenExpired)
            {
                AccessToken = accessToken;
                RefreshToken = refreshToken;
                TokenExpired = tokenExpired;
            }

            public string? AccessToken { get; }
            public string? RefreshToken { get; }
            public string? TokenExpired { get; }
        }
    }
}