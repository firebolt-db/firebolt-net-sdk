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
        public string UserName { get; }

        /// <summary>
        /// Gets the password.
        /// </summary>
        public string? Password { get; }

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

        internal FireboltConnectionSettings(FireboltConnectionStringBuilder builder)
        {
            if (string.IsNullOrWhiteSpace(builder.Database))
                throw new ArgumentException("The database is not defined.", nameof(builder));

            UserName = builder.UserName;
            Password = string.IsNullOrEmpty(builder.Password) ? null : builder.Password;
            Database = string.IsNullOrEmpty(builder.Database) ? null : builder.Database;
            Endpoint = string.IsNullOrEmpty(builder.Endpoint) ? null : builder.Endpoint;
            Account = string.IsNullOrEmpty(builder.Account) ? null : builder.Account;
            Engine = string.IsNullOrEmpty(builder.Engine) ? null : builder.Engine;
        }
    }
}
