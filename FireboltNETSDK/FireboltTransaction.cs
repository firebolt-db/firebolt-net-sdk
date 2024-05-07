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
using System.Runtime.CompilerServices;
using IsolationLevel = System.Data.IsolationLevel;


[assembly: InternalsVisibleTo("FireboltDotNetSdk.Tests")]
namespace FireboltDotNetSdk.Client
{
    /// <summary>
    /// Represents a connection to a Firebolt database. This class cannot be inherited.
    /// </summary>
    internal sealed class FireboltTransaction : DbTransaction
    {
        private DbConnection _dbConnection;
        internal FireboltTransaction(DbConnection connection) : base()
        {
            _dbConnection = connection;
        }

        /// <summary>
        /// Isolation level of the transaction. Due to transaction are not supported and this implementation is simulation the return value is <see cref="IsolationLevel.Unspecified"/>
        /// </summary>
        /// <returns>The isolation level for this transaction: <see cref="IsolationLevel.Unspecified"/>.</returns>
        public override IsolationLevel IsolationLevel { get => IsolationLevel.Unspecified; }

        /// <summary>
        /// Gets DBConnection used for creation of current instance.
        /// </summary>
        /// <returns>Currend DBConnection.</returns>
        protected override DbConnection? DbConnection { get => _dbConnection; }

        /// <summary>
        /// Empty NOP implementation - does nothing.
        /// </summary>
        public override void Commit() { }

        /// <summary>
        /// Throws NotImplementedException to indicate that rollback is not supported.
        /// </summary>
        public override void Rollback()
        {
            throw new NotImplementedException();
        }

    }
}
