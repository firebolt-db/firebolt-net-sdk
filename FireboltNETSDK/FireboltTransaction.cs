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
using FireboltDotNetSdk.Exception;

[assembly: InternalsVisibleTo("FireboltDotNetSdk.Tests")]
namespace FireboltDotNetSdk.Client
{
    internal sealed class FireboltTransaction : DbTransaction
    {
        private readonly FireboltConnection _dbConnection;
        private bool _isDisposed = false;

        internal FireboltTransaction(FireboltConnection connection, IsolationLevel isolationLevel) : base()
        {
            _dbConnection = connection ?? throw new ArgumentNullException(nameof(connection));
            IsolationLevel = isolationLevel;

            BeginTransactionAsync().GetAwaiter().GetResult();
        }

        public override IsolationLevel IsolationLevel { get; }

        protected override DbConnection? DbConnection => _dbConnection;

        public override void Commit()
        {
            CommitAsync().GetAwaiter().GetResult();
        }

        public override async Task CommitAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            ThrowIfCompleted();

            try
            {
                await using var command = _dbConnection.CreateCommand();
                command.CommandText = "COMMIT";
                await command.ExecuteNonQueryAsync(cancellationToken);
                IsCommitted = true;
            }
            catch (System.Exception ex)
            {
                throw new FireboltException("Failed to commit transaction", ex);
            }
        }

        public override void Rollback()
        {
            RollbackAsync().GetAwaiter().GetResult();
        }

        public override async Task RollbackAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            ThrowIfCompleted();

            try
            {
                await using var command = _dbConnection.CreateCommand();
                command.CommandText = "ROLLBACK";
                await command.ExecuteNonQueryAsync(cancellationToken);
                IsRolledBack = true;
            }
            catch (System.Exception ex)
            {
                throw new FireboltException("Failed to rollback transaction", ex);
            }
        }

        private bool IsCompleted => IsCommitted || IsRolledBack;

        private bool IsCommitted { get; set; }

        private bool IsRolledBack { get; set; }

        private async Task BeginTransactionAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                await using var command = _dbConnection.CreateCommand();
                command.CommandText = "BEGIN TRANSACTION";
                await command.ExecuteNonQueryAsync(cancellationToken);
            }
            catch (System.Exception ex)
            {
                throw new FireboltException("Failed to begin transaction", ex);
            }
        }

        private void ThrowIfDisposed()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException(nameof(FireboltTransaction));
            }
        }

        private void ThrowIfCompleted()
        {
            if (IsCompleted)
            {
                throw new FireboltException(
                    IsCommitted
                        ? "The transaction has already been committed."
                        : "The transaction has already been rolled back.");
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (!_isDisposed && disposing)
            {
                try
                {
                    // If the transaction is still active, roll it back
                    if (!IsCompleted)
                    {
                        Rollback();
                    }
                }
                finally
                {
                    _isDisposed = true;
                }
            }
            base.Dispose(disposing);
        }
    }
}
