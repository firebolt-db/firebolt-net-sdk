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

using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Text.RegularExpressions;
using FireboltDotNetSdk.Exception;

namespace FireboltDotNetSdk.Client
{
    /// <summary>
    /// Provides a set of methods for working with connection settings and connection strings.
    /// </summary>
    public class FireboltConnectionStringBuilder : DbConnectionStringBuilder
    {
        private static readonly HashSet<string> AllProperties;
        private static readonly string EngineNameKey = "engine_name";
        private static readonly string EngineUrlKey = "engine_url";

        /// <summary>
        /// Gets or sets the name of the user.
        /// </summary>
        /// <returns>The name of the user.</returns>
        public string UserName
        {
            get => GetString(nameof(UserName)) ?? throw new FireboltException("Missing username");
            init => this[nameof(UserName)] = value;
        }

        /// <summary>
        /// Gets or sets the password.
        /// </summary>
        /// <returns>The password.</returns>
        [DisallowNull]
        public string? Password
        {
            get => GetString(nameof(Password));
            init
            {
                if (string.IsNullOrEmpty(value))
                    throw new ArgumentException("Value cannot be null or empty.", nameof(value));
                this[nameof(Password)] = Regex.Escape(value);
            }
        }

        /// <summary>
        /// Gets or sets the name of the default database.
        /// </summary>
        /// <returns>The name of the default database. <see langword="null"/> if the default database in not specified.</returns>
        public string? Database
        {
            get => GetString(nameof(Database));
            init => this[nameof(Database)] = value;
        }

        /// <summary>
        /// Get the name of the default Endpoint.
        /// </summary>
        public string? Endpoint
        {
            get => GetString(nameof(Endpoint));
            init => this[nameof(Endpoint)] = value;
        }

        /// <summary>
        /// Get the name of the default Account.
        /// </summary>
        public string? Account
        {
            get => GetString(nameof(Account));
            init => this[nameof(Account)] = value;
        }

        /// <summary>
        /// Get the name of the engine.
        /// </summary>
        public string? EngineName
        {
            get => GetString(EngineNameKey);
            init => this[EngineNameKey] = value;
        }

        /// <summary>
        /// Get the name of the engine.
        /// </summary>
        public string? EngineUrl
        {
            get => GetString(EngineUrlKey);
            init => this[EngineUrlKey] = value;
        }

        static FireboltConnectionStringBuilder()
        {
            AllProperties = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                nameof(Database),
                nameof(Password),
                nameof(UserName),
                nameof(Endpoint),
                nameof(Account),
                EngineNameKey,
                EngineUrlKey,
            };
        }

        /// <summary>
        /// Initializes a new instance of <see cref="FireBoltConnectionStringBuilder"/> with the settings specified in the connection string.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public FireboltConnectionStringBuilder(string connectionString)
        {
            ConnectionString = connectionString;
        }

        /// <inheritdoc/>
        [AllowNull]
        public override object this[string keyword]
        {
            get => base[keyword];
            set
            {
                if (!AllProperties.Contains(keyword))
                    throw new ArgumentException($"\"{keyword}\" is not a valid connection parameter name.", nameof(keyword));

                base[keyword] = value;
            }
        }

        /// <summary>
        /// Creates and returns a new instance of the <see cref="FireBoltConnectionSettings"/>.
        /// </summary>
        /// <returns>A new instance of the <see cref="FireBoltConnectionSettings"/>.</returns>
        public FireboltConnectionSettings BuildSettings()
        {
            return new FireboltConnectionSettings(this);
        }

        private string? GetString(string key)
        {
            return TryGetValue(key, out var value) ? (string)value : null;
        }
    }
}
