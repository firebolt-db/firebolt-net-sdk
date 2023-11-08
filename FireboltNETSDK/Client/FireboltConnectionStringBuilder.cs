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

namespace FireboltDotNetSdk.Client
{
    /// <summary>
    /// Provides a set of methods for working with connection settings and connection strings.
    /// </summary>
    public class FireboltConnectionStringBuilder : DbConnectionStringBuilder
    {
        private static readonly HashSet<string> AllProperties;

        /// <summary>
        /// Gets or sets the name of the user.
        /// </summary>
        /// <returns>The name of the user.</returns>
        public string? UserName
        {
            get => GetString(nameof(UserName));
            init => this[nameof(UserName)] = value;
        }

        /// <summary>
        /// Gets or sets the password.
        /// </summary>
        /// <returns>The password.</returns>
        [DisallowNull]
        public string? Password
        {
            // get => GetString(nameof(Password));
            get => GetString(nameof(Password));
            init => this[nameof(Password)] = value != null ? Regex.Escape(value) : value;
        }

        /// <summary>
        /// Gets or sets the name of the user.
        /// </summary>
        /// <returns>The name of the user.</returns>
        public string? ClientId
        {
            get => GetString(nameof(ClientId));
            init => this[nameof(ClientId)] = value;
        }

        /// <summary>
        /// Gets or sets the password.
        /// </summary>
        /// <returns>The password.</returns>
        public string? ClientSecret
        {
            get => GetString(nameof(ClientSecret));
            init => this[nameof(ClientSecret)] = value != null ? Regex.Escape(value) : value;
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
        /// Get the name of the default Environment.
        /// </summary>
        public string? Env
        {
            get => GetString(nameof(Env));
            init => this[nameof(Env)] = value;
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
        public string? Engine
        {
            get => GetString(nameof(Engine));
            init => this[nameof(Engine)] = value;
        }

        public int Version
        {
            get;
            private set;
        }

        static FireboltConnectionStringBuilder()
        {
            AllProperties = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                nameof(Database),
                nameof(UserName),
                nameof(Password),
                nameof(ClientId),
                nameof(ClientSecret),
                nameof(Endpoint),
                nameof(Account),
                nameof(Engine),
                nameof(Env),
            };
        }

        /// <summary>
        /// Initializes a new instance of <see cref="FireBoltConnectionStringBuilder"/> with the settings specified in the connection string.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public FireboltConnectionStringBuilder(string connectionString)
        {
            ConnectionString = connectionString;
            InitVersion();
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

        private void InitVersion()
        {
            if (ClientId != null && ClientSecret != null && UserName == null && Password == null)
            {
                Version = 2;
                return;
            }
            Version = BuildSettings().Principal.Contains("@") ? 1 : 2;
        }
    }
}
