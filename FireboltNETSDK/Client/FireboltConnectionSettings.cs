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

        internal FireboltConnectionSettings(FireboltConnectionStringBuilder builder)
        {
            ConnectionString = builder.ConnectionString;
            ValidateValues(nameof(builder.UserName), nameof(builder.ClientId), builder.UserName, builder.ClientId);
            Principal = GetNotNullValue(builder.UserName, builder.ClientId);
            ValidateValues(nameof(builder.Password), nameof(builder.ClientSecret), builder.Password, builder.ClientSecret);
            Secret = GetNotNullValue(builder.Password, builder.ClientSecret);
            Database = string.IsNullOrEmpty(builder.Database) ? null : builder.Database;
            Account = builder.Account;
            Engine = string.IsNullOrEmpty(builder.Engine) ? null : builder.Engine;
            (Endpoint, Env) = ResolveEndpointAndEnv(builder);
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
                throw new FireboltException(
            "Configuration error: environment " +
            $"{env} and endpoint {endpoint} are incompatible"
        );
            }
            if (env == null && endpoint_env != null)
            {
                env = endpoint_env;
            }
            return (endpoint ?? Constant.DEFAULT_ENDPOINT, env ?? Constant.DEFAULT_ENV);
        }

        private string GetNotNullValue(string? firstValue, string? secondValue)
        {
            return new string?[] { firstValue, secondValue }.Where(s => !string.IsNullOrEmpty(s)).Select(s => s!).ToArray()[0];
        }

        private void ValidateValues(string firstName, string secondName, string? firstValue, string? secondValue)
        {
            string[] notNullValues = new string?[] { firstValue, secondValue }.Where(s => !string.IsNullOrEmpty(s)).Select(s => s!).ToArray();
            int notNullCount = notNullValues.Count();
            switch (notNullCount)
            {
                case 0: throw new FireboltException($"Either {firstName} or {secondName} parameter is missing in the connection string");
                case 2: throw new FireboltException($"Ambiguous values of {firstName} and {secondName}. Use only one of them");
                // this cannot happen. Added to satisfy compiler and to prevent future bugs.
                default: throw new InvalidOperationException($"{notNullCount} values for {firstName} or {secondName}. Only one value is legal");
            }
        }
    }
}
