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
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;

namespace FireboltDotNetSdk.Client
{
    /// <summary>
    /// Represents an immutable set of properties applied to the connection. This class can't be inherited.
    /// </summary>
    public sealed class FireboltConnectionSettings
    {
        /// <summary>
        /// Gets the name of the user.
        /// </summary>
        public string ClientId { get; }

        /// <summary>
        /// Gets the password.
        /// </summary>
        public string ClientSecret { get; }

        /// <summary>
        /// Get the name of the default database.
        /// </summary>
        public string? Database { get; }

        /// <summary>
        /// Get the name of the default Endpoint.
        /// </summary>
        public string Endpoint { get; }

        /// <summary>
        /// Get the name of the default Account.
        /// </summary>
        public string Account { get; }

        /// <summary>
        /// Get the name of the engine
        /// </summary>
        public string? Engine { get; }

        /// Get the name of the environment.
        /// </summary>
        public string? Env { get; }

        internal FireboltConnectionSettings(FireboltConnectionStringBuilder builder)
        {

            ClientId = builder.ClientId;
            ClientSecret = builder.ClientSecret;
            Database = string.IsNullOrEmpty(builder.Database) ? null : builder.Database;
            Account = builder.Account;
            Engine = string.IsNullOrEmpty(builder.Engine) ? null : builder.Engine;
            (Endpoint, Env) = this.ResolveEndpointAndEnv(builder);
        }

        static string? ExtractEndpointEnv(string endpoint)
        {
            // Remove http:// or https:// prefix if present
            endpoint = endpoint.Split("://")[^1];
            var endpoint_parts = endpoint.Split(".");
            // Verify the expected 
            if (
            endpoint_parts.Length == 4 &&
            endpoint_parts[0] == "api" &&
            endpoint_parts[2] == "firebolt" &&
            endpoint_parts[3] == "io"
            )
            {
                return endpoint_parts[1];
            }
            return null;
        }

        (string, string) ResolveEndpointAndEnv(FireboltConnectionStringBuilder builder)
        {
            var endpoint = string.IsNullOrEmpty(builder.Endpoint) ? null : builder.Endpoint;
            var env = string.IsNullOrEmpty(builder.Env) ? null : builder.Env;
            var endpoint_env = endpoint != null ? ExtractEndpointEnv(endpoint) : null;

            if (env != null && endpoint_env != null && env != endpoint_env)
            {
                throw new FireboltException(
            "Configuration error: environment " +
            $"{env} and endpoint {endpoint} are incompatible"
        );
            }
            if (env == null && endpoint_env != null)
            {
                env = endpoint_env;
            }
            System.Console.WriteLine($"Endpoint: {endpoint}");
            System.Console.WriteLine($"Env: {env}");
            return (endpoint ?? Constant.DEFAULT_ENDPOINT, env ?? Constant.DEFAULT_ENV);
        }
    }
}
