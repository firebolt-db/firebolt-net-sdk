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

using FireboltDotNetSdk.Client;
using static FireboltDotNetSdk.Client.FireRequest;
using static FireboltDotNetSdk.Client.FireResponse;

namespace FireboltDotNetSdk
{
    public class FireboltClient
    {
        private string BaseUrl { get; set; }
        private string? Token {  get; set; }

        private FireboltCommand FireboltClientInternal { get; set; }

        /// <summary>
        /// Creates a new instance of the Firebolt client.
        /// </summary>
        /// <param name="baseUrl"></param>
        public FireboltClient(string baseUrl)
        {
            this.BaseUrl = baseUrl;
            this.FireboltClientInternal = new FireboltCommand(this.BaseUrl);
        }

        /// <summary>
        /// Sets the token used for authentication.
        /// </summary>
        public void ClearToken()
        {
            this.Token = null;
            this.FireboltClientInternal.Token = null;
        }

        /// <summary>
        /// Sets the token used for authentication.
        /// </summary>
        /// <param name="token"></param>
        public void SetToken(LoginResponse token)
        {
            this.Token = token.Access_token;
            this.FireboltClientInternal.Token = token.Access_token;
            this.FireboltClientInternal.RefreshToken = token.Refresh_token;
            this.FireboltClientInternal.TokenExpired = token.Expires_in;
        }

        /// <summary>
        /// Authenticates the user with Firebolt.
        /// </summary>
        /// <param name="loginRequest"></param>
        /// <returns></returns>
        public Task<LoginResponse> Login(LoginRequest loginRequest)
        {
            return FireboltClientInternal.AuthV1LoginAsync(loginRequest);
        }

        /// <summary>
        /// Returns engine URL by database name given.
        /// </summary>
        /// <param name="databaseName">Name of the database.</param>
        /// <returns>A successful response.</returns>
        public Task<GetEngineUrlByDatabaseNameResponse> GetEngineUrlByDatabaseName(string? databaseName, string? engine,string? account)
        {
            return FireboltClientInternal.CoreV1GetEngineUrlByDatabaseNameAsync(databaseName,engine, account, CancellationToken.None);
        }

        /// <summary>
        /// Executes a SQL query
        /// </summary>
        /// <param name="engineEndpoint">Engine endpoint (URL)</param>
        /// <param name="databaseName">Database name</param>
        /// <param name="query">SQL query to execute</param>
        /// <returns>A successful response.</returns>
        public Task<string?> ExecuteQuery(string? engineEndpoint, string databaseName, string query)
        {
            return FireboltClientInternal.ExecuteQueryAsync(engineEndpoint, databaseName, query, CancellationToken.None);
        }
    }
}
