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
using System.Text.RegularExpressions;

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
        public string Principal { get; }

        /// <summary>
        /// Gets the password.
        /// </summary>
        public string Secret { get; }

        /// <summary>
        /// Get the name of the default database.
        /// </summary>
        public string? Database { get; }

        /// <summary>
        /// Get the name of the default Endpoint.
        /// </summary>
        public string? Endpoint { get; }

        /// <summary>
        /// Get the name of the default Account.
        /// </summary>
        public string? Account { get; }

        /// <summary>
        /// Get the name of the engine
        /// </summary>
        public string? Engine { get; }

        /// Get the name of the environment.
        /// </summary>
        public string? Env { get; }
        public string? ConnectionString { get; }
        public TokenStorageType TokenStorageType { get; }

        internal FireboltConnectionSettings(FireboltConnectionStringBuilder builder)
        {
            ConnectionString = builder.ConnectionString;
            ValidateValues(builder);
            Principal = GetNotNullValue(builder.UserName, builder.ClientId);
            Secret = GetNotNullValue(builder.Password, builder.ClientSecret);
            Database = string.IsNullOrEmpty(builder.Database) ? null : builder.Database;
            Account = builder.Account;
            Engine = string.IsNullOrEmpty(builder.Engine) ? null : builder.Engine;
            (Endpoint, Env) = ResolveEndpointAndEnv(builder);
            TokenStorageType = builder.TokenStorage ?? TokenStorageType.Memory;
        }

        static string? ExtractEndpointEnv(string endpoint)
        {
            var pattern = new Regex(@"(\w*://)?api\.(?<env>\w+)\.firebolt\.io");
            var match = pattern.Match(endpoint);
            return match.Success ? match.Groups["env"].Value : null;
        }

        (string, string) ResolveEndpointAndEnv(FireboltConnectionStringBuilder builder)
        {
            var endpoint = string.IsNullOrEmpty(builder.Endpoint) ? null : builder.Endpoint;
            var env = string.IsNullOrEmpty(builder.Env) ? null : builder.Env;
            var endpoint_env = endpoint != null ? ExtractEndpointEnv(endpoint) : null;

            if (env != null && endpoint_env != null && env != endpoint_env)
            {
                throw new FireboltException($"Configuration error: environment {env} and endpoint {endpoint} are incompatible");
            }
            if (env == null && endpoint_env != null)
            {
                env = endpoint_env;
            }
            return (endpoint ?? Constant.DEFAULT_ENDPOINT, env ?? Constant.DEFAULT_ENV);
        }

        private string GetNotNullValue(string? firstValue, string? secondValue)
        {
            return GetNotNullValues(firstValue, secondValue)[0];
        }

        private string[] GetNotNullValues(string? firstValue, string? secondValue)
        {
            return new string?[] { firstValue, secondValue }.Where(s => !string.IsNullOrEmpty(s)).Select(s => s!).ToArray();
        }

        private void ValidateValues(FireboltConnectionStringBuilder builder)
        {
            if (AreBothProvided(builder.UserName, builder.ClientId) || AreBothMissing(builder.UserName, builder.ClientId))
            {
                throw new FireboltException("Configuration error: either UserName or ClientId must be provided but not both");
            }
            if (AreBothProvided(builder.Password, builder.ClientSecret) || AreBothMissing(builder.Password, builder.ClientSecret))
            {
                throw new FireboltException("Configuration error: either Password or ClientSecret must be provided but not both");
            }
            if (builder.Version == 2 && builder.Account == null)
            {
                throw new FireboltException("Account parameter is missing in the connection string");
            }
        }

        private bool AreBothMissing(string? firstValue, string? secondValue)
        {
            return string.IsNullOrEmpty(firstValue) && string.IsNullOrEmpty(secondValue);
        }

        private bool AreBothProvided(string? firstValue, string? secondValue)
        {
            return !string.IsNullOrEmpty(firstValue) && !string.IsNullOrEmpty(secondValue);
        }
    }
}
