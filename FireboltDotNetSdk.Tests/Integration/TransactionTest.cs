using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using FireboltNETSDK.Exception;

namespace FireboltDotNetSdk.Tests.Integration
{
    [TestFixture]
    [Category("Integration")]
    internal class TransactionTest : IntegrationTest
    {
        private const string TestTable = "transaction_test";

        [OneTimeSetUp]
        public async Task OneTimeSetUp()
        {
            await using var connection = new FireboltConnection(ConnectionString());
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"CREATE TABLE IF NOT EXISTS {TestTable} (id INT, name TEXT) PRIMARY INDEX id";
            await command.ExecuteNonQueryAsync();
        }

        [OneTimeTearDown]
        public async Task OneTimeTearDown()
        {
            await using var connection = new FireboltConnection(ConnectionString());
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP TABLE IF EXISTS {TestTable}";
            await command.ExecuteNonQueryAsync();
        }

        [Test]
        public async Task TestTransactionCommit()
        {
            await using var connection = new FireboltConnection(ConnectionString());
            await connection.OpenAsync();

            await using var transaction = (FireboltTransaction)await connection.BeginTransactionAsync();

            await using var insertCmd = connection.CreateCommand();
            insertCmd.CommandText = $"INSERT INTO {TestTable} VALUES (1, 'test')";
            await insertCmd.ExecuteNonQueryAsync();

            await using var connection2 = new FireboltConnection(ConnectionString());
            await connection2.OpenAsync();
            await using var selectCmd = connection2.CreateCommand();
            selectCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 1";
            var countBeforeCommit = await selectCmd.ExecuteScalarAsync();
            Assert.That(countBeforeCommit, Is.EqualTo(0L));

            await transaction.CommitAsync();

            await using var selectAfterCmd = connection2.CreateCommand();
            selectAfterCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 1";
            var countAfterCommit = await selectAfterCmd.ExecuteScalarAsync();
            Assert.That(countAfterCommit, Is.EqualTo(1L));
        }

        [Test]
        public async Task TestTransactionRollback()
        {
            await using var connection = new FireboltConnection(ConnectionString());
            await connection.OpenAsync();

            await using var transaction = (FireboltTransaction)await connection.BeginTransactionAsync();

            await using var insertCmd = connection.CreateCommand();
            insertCmd.Transaction = transaction;
            insertCmd.CommandText = $"INSERT INTO {TestTable} VALUES (2, 'test')";
            await insertCmd.ExecuteNonQueryAsync();

            await using var connection2 = new FireboltConnection(ConnectionString());
            await connection2.OpenAsync();
            await using var selectCmd = connection2.CreateCommand();
            selectCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 2";
            var countBeforeRollback = await selectCmd.ExecuteScalarAsync();
            Assert.That(countBeforeRollback, Is.EqualTo(0L));

            await transaction.RollbackAsync();

            await using var selectAfterCmd = connection2.CreateCommand();
            selectAfterCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 2";
            var countAfterRollback = await selectAfterCmd.ExecuteScalarAsync();
            Assert.That(countAfterRollback, Is.EqualTo(0L));
        }

        [Test]
        public async Task TestTransactionRollbackWithDispose()
        {
            await using var connection = new FireboltConnection(ConnectionString());
            await using var connection2 = new FireboltConnection(ConnectionString());
            await connection.OpenAsync();
            await connection2.OpenAsync();

            await using (var transaction = (FireboltTransaction)await connection.BeginTransactionAsync())
            {
                await using var insertCmd = connection.CreateCommand();
                insertCmd.Transaction = transaction;
                insertCmd.CommandText = $"INSERT INTO {TestTable} VALUES (3, 'test')";
                await insertCmd.ExecuteNonQueryAsync();

                await using var selectCmd = connection2.CreateCommand();
                selectCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 3";
                var countBeforeRollback = await selectCmd.ExecuteScalarAsync();
                Assert.That(countBeforeRollback, Is.EqualTo(0L));
            }


            await using var selectAfterCmd = connection2.CreateCommand();
            selectAfterCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 3";
            var countAfterDispose = await selectAfterCmd.ExecuteScalarAsync();
            Assert.That(countAfterDispose, Is.EqualTo(0L));
        }

        [Test]
        public async Task TestTransactionIsolation()
        {
            await using var connection1 = new FireboltConnection(ConnectionString());
            await using var connection2 = new FireboltConnection(ConnectionString());
            await connection1.OpenAsync();
            await connection2.OpenAsync();

            await using var transaction = (FireboltTransaction)await connection1.BeginTransactionAsync();

            await using var insertCmd = connection1.CreateCommand();
            insertCmd.CommandText = $"INSERT INTO {TestTable} VALUES (4, 'isolated')";
            await insertCmd.ExecuteNonQueryAsync();

            await using var selectFromConn1 = connection1.CreateCommand();
            selectFromConn1.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 4";
            var countFromConn1 = await selectFromConn1.ExecuteScalarAsync();
            Assert.That(countFromConn1, Is.EqualTo(1L));

            await using var selectFromConn2 = connection2.CreateCommand();
            selectFromConn2.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 4";
            var countFromConn2 = await selectFromConn2.ExecuteScalarAsync();
            Assert.That(countFromConn2, Is.EqualTo(0L));

            await transaction.CommitAsync();

            await using var selectAfterCommit1 = connection1.CreateCommand();
            selectAfterCommit1.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 4";
            var countAfterCommit1 = await selectAfterCommit1.ExecuteScalarAsync();
            Assert.That(countAfterCommit1, Is.EqualTo(1L));

            await using var selectAfterCommit2 = connection2.CreateCommand();
            selectAfterCommit2.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 4";
            var countAfterCommit2 = await selectAfterCommit2.ExecuteScalarAsync();
            Assert.That(countAfterCommit2, Is.EqualTo(1L));
        }

        [Test]
        public async Task TestMultipleInserts()
        {
            await using var connection = new FireboltConnection(ConnectionString());
            await connection.OpenAsync();

            await using var transaction = (FireboltTransaction)await connection.BeginTransactionAsync();

            await using var insertCmd1 = connection.CreateCommand();
            insertCmd1.CommandText = $"INSERT INTO {TestTable} VALUES (5, 'first')";
            await insertCmd1.ExecuteNonQueryAsync();

            await using var insertCmd2 = connection.CreateCommand();
            insertCmd2.CommandText = $"INSERT INTO {TestTable} VALUES (6, 'second')";
            await insertCmd2.ExecuteNonQueryAsync();

            await using var connection2 = new FireboltConnection(ConnectionString());
            await connection2.OpenAsync();
            await using var selectCmd = connection2.CreateCommand();
            selectCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id IN (5, 6)";
            var countBeforeCommit = await selectCmd.ExecuteScalarAsync();
            Assert.That(countBeforeCommit, Is.EqualTo(0L));

            await transaction.CommitAsync();

            await using var selectAfterCmd = connection2.CreateCommand();
            selectAfterCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id IN (5, 6)";
            var countAfterCommit = await selectAfterCmd.ExecuteScalarAsync();
            Assert.That(countAfterCommit, Is.EqualTo(2L));
        }

        [Test]
        public async Task TestNestedTransactionThrows()
        {
            await using var connection = new FireboltConnection(ConnectionString());
            await connection.OpenAsync();

            await using var transaction1 = await connection.BeginTransactionAsync();

            Assert.ThrowsAsync<FireboltException>(async () =>
            {
                await using var transaction2 = await connection.BeginTransactionAsync();
                await Task.CompletedTask;
            });
        }

        [Test]
        public void TestTransactionCommitSync()
        {
            using var connection = new FireboltConnection(ConnectionString());
            connection.Open();

            using var transaction = (FireboltTransaction)connection.BeginTransaction();

            using var insertCmd = connection.CreateCommand();
            insertCmd.Transaction = transaction;
            insertCmd.CommandText = $"INSERT INTO {TestTable} VALUES (10, 'test_sync')";
            insertCmd.ExecuteNonQuery();

            using var connection2 = new FireboltConnection(ConnectionString());
            connection2.Open();
            using var selectCmd = connection2.CreateCommand();
            selectCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 10";
            var countBeforeCommit = selectCmd.ExecuteScalar();
            Assert.That(countBeforeCommit, Is.EqualTo(0L));

            transaction.Commit();

            using var selectAfterCmd = connection2.CreateCommand();
            selectAfterCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 10";
            var countAfterCommit = selectAfterCmd.ExecuteScalar();
            Assert.That(countAfterCommit, Is.EqualTo(1L));
        }

        [Test]
        public void TestTransactionRollbackSync()
        {
            using var connection = new FireboltConnection(ConnectionString());
            connection.Open();

            using var transaction = (FireboltTransaction)connection.BeginTransaction();

            using var insertCmd = connection.CreateCommand();
            insertCmd.Transaction = transaction;
            insertCmd.CommandText = $"INSERT INTO {TestTable} VALUES (11, 'test_sync')";
            insertCmd.ExecuteNonQuery();

            using var connection2 = new FireboltConnection(ConnectionString());
            connection2.Open();
            using var selectCmd = connection2.CreateCommand();
            selectCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 11";
            var countBeforeRollback = selectCmd.ExecuteScalar();
            Assert.That(countBeforeRollback, Is.EqualTo(0L));

            transaction.Rollback();

            using var selectAfterCmd = connection2.CreateCommand();
            selectAfterCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 11";
            var countAfterRollback = selectAfterCmd.ExecuteScalar();
            Assert.That(countAfterRollback, Is.EqualTo(0L));
        }

        [Test]
        public void TestTransactionIsolationSync()
        {
            using var connection1 = new FireboltConnection(ConnectionString());
            using var connection2 = new FireboltConnection(ConnectionString());
            connection1.Open();
            connection2.Open();

            using var transaction = (FireboltTransaction)connection1.BeginTransaction();

            using var insertCmd = connection1.CreateCommand();
            insertCmd.Transaction = transaction;
            insertCmd.CommandText = $"INSERT INTO {TestTable} VALUES (12, 'isolated_sync')";
            insertCmd.ExecuteNonQuery();

            using var selectFromConn1 = connection1.CreateCommand();
            selectFromConn1.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 12";
            var countFromConn1 = selectFromConn1.ExecuteScalar();
            Assert.That(countFromConn1, Is.EqualTo(1L));

            using var selectFromConn2 = connection2.CreateCommand();
            selectFromConn2.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 12";
            var countFromConn2 = selectFromConn2.ExecuteScalar();
            Assert.That(countFromConn2, Is.EqualTo(0L));

            transaction.Commit();

            using var selectAfterCommit1 = connection1.CreateCommand();
            selectAfterCommit1.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 12";
            var countAfterCommit1 = selectAfterCommit1.ExecuteScalar();
            Assert.That(countAfterCommit1, Is.EqualTo(1L));

            using var selectAfterCommit2 = connection2.CreateCommand();
            selectAfterCommit2.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 12";
            var countAfterCommit2 = selectAfterCommit2.ExecuteScalar();
            Assert.That(countAfterCommit2, Is.EqualTo(1L));
        }

        [Test]
        public void TestMultipleInsertsSync()
        {
            using var connection = new FireboltConnection(ConnectionString());
            connection.Open();

            using var transaction = (FireboltTransaction)connection.BeginTransaction();

            using var insertCmd1 = connection.CreateCommand();
            insertCmd1.Transaction = transaction;
            insertCmd1.CommandText = $"INSERT INTO {TestTable} VALUES (13, 'first_sync')";
            insertCmd1.ExecuteNonQuery();

            using var insertCmd2 = connection.CreateCommand();
            insertCmd2.Transaction = transaction;
            insertCmd2.CommandText = $"INSERT INTO {TestTable} VALUES (14, 'second_sync')";
            insertCmd2.ExecuteNonQuery();

            using var connection2 = new FireboltConnection(ConnectionString());
            connection2.Open();
            using var selectCmd = connection2.CreateCommand();
            selectCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id IN (13, 14)";
            var countBeforeCommit = selectCmd.ExecuteScalar();
            Assert.That(countBeforeCommit, Is.EqualTo(0L));

            transaction.Commit();

            using var selectAfterCmd = connection2.CreateCommand();
            selectAfterCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id IN (13, 14)";
            var countAfterCommit = selectAfterCmd.ExecuteScalar();
            Assert.That(countAfterCommit, Is.EqualTo(2L));
        }

        [Test]
        public void TestNestedTransactionThrowsSync()
        {
            using var connection = new FireboltConnection(ConnectionString());
            connection.Open();

            using var transaction1 = connection.BeginTransaction();

            Assert.Throws<FireboltException>(() =>
            {
                using var transaction2 = connection.BeginTransaction();
            });
        }

        // SQL Command Tests (Async)
        [Test]
        public async Task TestTransactionCommitSql()
        {
            await using var connection = new FireboltConnection(ConnectionString());
            await connection.OpenAsync();

            await using var beginCmd = connection.CreateCommand();
            beginCmd.CommandText = "BEGIN TRANSACTION";
            await beginCmd.ExecuteNonQueryAsync();

            await using var insertCmd = connection.CreateCommand();
            insertCmd.CommandText = $"INSERT INTO {TestTable} VALUES (20, 'test_sql')";
            await insertCmd.ExecuteNonQueryAsync();

            await using var connection2 = new FireboltConnection(ConnectionString());
            await connection2.OpenAsync();
            await using var selectCmd = connection2.CreateCommand();
            selectCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 20";
            var countBeforeCommit = await selectCmd.ExecuteScalarAsync();
            Assert.That(countBeforeCommit, Is.EqualTo(0L));

            await using var commitCmd = connection.CreateCommand();
            commitCmd.CommandText = "COMMIT";
            await commitCmd.ExecuteNonQueryAsync();

            await using var selectAfterCmd = connection2.CreateCommand();
            selectAfterCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 20";
            var countAfterCommit = await selectAfterCmd.ExecuteScalarAsync();
            Assert.That(countAfterCommit, Is.EqualTo(1L));
        }

        [Test]
        public async Task TestTransactionRollbackSql()
        {
            await using var connection = new FireboltConnection(ConnectionString());
            await connection.OpenAsync();

            await using var beginCmd = connection.CreateCommand();
            beginCmd.CommandText = "BEGIN TRANSACTION";
            await beginCmd.ExecuteNonQueryAsync();

            await using var insertCmd = connection.CreateCommand();
            insertCmd.CommandText = $"INSERT INTO {TestTable} VALUES (21, 'test_sql')";
            await insertCmd.ExecuteNonQueryAsync();

            await using var connection2 = new FireboltConnection(ConnectionString());
            await connection2.OpenAsync();
            await using var selectCmd = connection2.CreateCommand();
            selectCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 21";
            var countBeforeRollback = await selectCmd.ExecuteScalarAsync();
            Assert.That(countBeforeRollback, Is.EqualTo(0L));

            await using var rollbackCmd = connection.CreateCommand();
            rollbackCmd.CommandText = "ROLLBACK";
            await rollbackCmd.ExecuteNonQueryAsync();

            await using var selectAfterCmd = connection2.CreateCommand();
            selectAfterCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 21";
            var countAfterRollback = await selectAfterCmd.ExecuteScalarAsync();
            Assert.That(countAfterRollback, Is.EqualTo(0L));
        }

        [Test]
        public async Task TestTransactionIsolationSql()
        {
            await using var connection1 = new FireboltConnection(ConnectionString());
            await using var connection2 = new FireboltConnection(ConnectionString());
            await connection1.OpenAsync();
            await connection2.OpenAsync();

            await using var beginCmd = connection1.CreateCommand();
            beginCmd.CommandText = "BEGIN TRANSACTION";
            await beginCmd.ExecuteNonQueryAsync();

            await using var insertCmd = connection1.CreateCommand();
            insertCmd.CommandText = $"INSERT INTO {TestTable} VALUES (22, 'isolated_sql')";
            await insertCmd.ExecuteNonQueryAsync();

            await using var selectFromConn1 = connection1.CreateCommand();
            selectFromConn1.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 22";
            var countFromConn1 = await selectFromConn1.ExecuteScalarAsync();
            Assert.That(countFromConn1, Is.EqualTo(1L));

            await using var selectFromConn2 = connection2.CreateCommand();
            selectFromConn2.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 22";
            var countFromConn2 = await selectFromConn2.ExecuteScalarAsync();
            Assert.That(countFromConn2, Is.EqualTo(0L));

            await using var commitCmd = connection1.CreateCommand();
            commitCmd.CommandText = "COMMIT";
            await commitCmd.ExecuteNonQueryAsync();

            await using var selectAfterCommit1 = connection1.CreateCommand();
            selectAfterCommit1.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 22";
            var countAfterCommit1 = await selectAfterCommit1.ExecuteScalarAsync();
            Assert.That(countAfterCommit1, Is.EqualTo(1L));

            await using var selectAfterCommit2 = connection2.CreateCommand();
            selectAfterCommit2.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 22";
            var countAfterCommit2 = await selectAfterCommit2.ExecuteScalarAsync();
            Assert.That(countAfterCommit2, Is.EqualTo(1L));
        }

        [Test]
        public async Task TestMultipleInsertsSql()
        {
            await using var connection = new FireboltConnection(ConnectionString());
            await connection.OpenAsync();

            await using var beginCmd = connection.CreateCommand();
            beginCmd.CommandText = "BEGIN TRANSACTION";
            await beginCmd.ExecuteNonQueryAsync();

            await using var insertCmd1 = connection.CreateCommand();
            insertCmd1.CommandText = $"INSERT INTO {TestTable} VALUES (23, 'first_sql')";
            await insertCmd1.ExecuteNonQueryAsync();

            await using var insertCmd2 = connection.CreateCommand();
            insertCmd2.CommandText = $"INSERT INTO {TestTable} VALUES (24, 'second_sql')";
            await insertCmd2.ExecuteNonQueryAsync();

            await using var connection2 = new FireboltConnection(ConnectionString());
            await connection2.OpenAsync();
            await using var selectCmd = connection2.CreateCommand();
            selectCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id IN (23, 24)";
            var countBeforeCommit = await selectCmd.ExecuteScalarAsync();
            Assert.That(countBeforeCommit, Is.EqualTo(0L));

            await using var commitCmd = connection.CreateCommand();
            commitCmd.CommandText = "COMMIT";
            await commitCmd.ExecuteNonQueryAsync();

            await using var selectAfterCmd = connection2.CreateCommand();
            selectAfterCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id IN (23, 24)";
            var countAfterCommit = await selectAfterCmd.ExecuteScalarAsync();
            Assert.That(countAfterCommit, Is.EqualTo(2L));
        }

        [Test]
        public async Task TestNestedTransactionThrowsSql()
        {
            await using var connection = new FireboltConnection(ConnectionString());
            await connection.OpenAsync();

            await using var beginCmd1 = connection.CreateCommand();
            beginCmd1.CommandText = "BEGIN TRANSACTION";
            await beginCmd1.ExecuteNonQueryAsync();

            Assert.ThrowsAsync<FireboltStructuredException>(async () =>
            {
                await using var beginCmd2 = connection.CreateCommand();
                beginCmd2.CommandText = "BEGIN TRANSACTION";
                await beginCmd2.ExecuteNonQueryAsync();
            });
        }

        // SQL Command Tests (Sync)
        [Test]
        public void TestTransactionCommitSqlSync()
        {
            using var connection = new FireboltConnection(ConnectionString());
            connection.Open();

            using var beginCmd = connection.CreateCommand();
            beginCmd.CommandText = "BEGIN TRANSACTION";
            beginCmd.ExecuteNonQuery();

            using var insertCmd = connection.CreateCommand();
            insertCmd.CommandText = $"INSERT INTO {TestTable} VALUES (30, 'test_sql_sync')";
            insertCmd.ExecuteNonQuery();

            using var connection2 = new FireboltConnection(ConnectionString());
            connection2.Open();
            using var selectCmd = connection2.CreateCommand();
            selectCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 30";
            var countBeforeCommit = selectCmd.ExecuteScalar();
            Assert.That(countBeforeCommit, Is.EqualTo(0L));

            using var commitCmd = connection.CreateCommand();
            commitCmd.CommandText = "COMMIT";
            commitCmd.ExecuteNonQuery();

            using var selectAfterCmd = connection2.CreateCommand();
            selectAfterCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 30";
            var countAfterCommit = selectAfterCmd.ExecuteScalar();
            Assert.That(countAfterCommit, Is.EqualTo(1L));
        }

        [Test]
        public void TestTransactionRollbackSqlSync()
        {
            using var connection = new FireboltConnection(ConnectionString());
            connection.Open();

            using var beginCmd = connection.CreateCommand();
            beginCmd.CommandText = "BEGIN TRANSACTION";
            beginCmd.ExecuteNonQuery();

            using var insertCmd = connection.CreateCommand();
            insertCmd.CommandText = $"INSERT INTO {TestTable} VALUES (31, 'test_sql_sync')";
            insertCmd.ExecuteNonQuery();

            using var connection2 = new FireboltConnection(ConnectionString());
            connection2.Open();
            using var selectCmd = connection2.CreateCommand();
            selectCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 31";
            var countBeforeRollback = selectCmd.ExecuteScalar();
            Assert.That(countBeforeRollback, Is.EqualTo(0L));

            using var rollbackCmd = connection.CreateCommand();
            rollbackCmd.CommandText = "ROLLBACK";
            rollbackCmd.ExecuteNonQuery();

            using var selectAfterCmd = connection2.CreateCommand();
            selectAfterCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 31";
            var countAfterRollback = selectAfterCmd.ExecuteScalar();
            Assert.That(countAfterRollback, Is.EqualTo(0L));
        }

        [Test]
        public void TestTransactionIsolationSqlSync()
        {
            using var connection1 = new FireboltConnection(ConnectionString());
            using var connection2 = new FireboltConnection(ConnectionString());
            connection1.Open();
            connection2.Open();

            using var beginCmd = connection1.CreateCommand();
            beginCmd.CommandText = "BEGIN TRANSACTION";
            beginCmd.ExecuteNonQuery();

            using var insertCmd = connection1.CreateCommand();
            insertCmd.CommandText = $"INSERT INTO {TestTable} VALUES (32, 'isolated_sql_sync')";
            insertCmd.ExecuteNonQuery();

            using var selectFromConn1 = connection1.CreateCommand();
            selectFromConn1.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 32";
            var countFromConn1 = selectFromConn1.ExecuteScalar();
            Assert.That(countFromConn1, Is.EqualTo(1L));

            using var selectFromConn2 = connection2.CreateCommand();
            selectFromConn2.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 32";
            var countFromConn2 = selectFromConn2.ExecuteScalar();
            Assert.That(countFromConn2, Is.EqualTo(0L));

            using var commitCmd = connection1.CreateCommand();
            commitCmd.CommandText = "COMMIT";
            commitCmd.ExecuteNonQuery();

            using var selectAfterCommit1 = connection1.CreateCommand();
            selectAfterCommit1.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 32";
            var countAfterCommit1 = selectAfterCommit1.ExecuteScalar();
            Assert.That(countAfterCommit1, Is.EqualTo(1L));

            using var selectAfterCommit2 = connection2.CreateCommand();
            selectAfterCommit2.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id = 32";
            var countAfterCommit2 = selectAfterCommit2.ExecuteScalar();
            Assert.That(countAfterCommit2, Is.EqualTo(1L));
        }

        [Test]
        public void TestMultipleInsertsSqlSync()
        {
            using var connection = new FireboltConnection(ConnectionString());
            connection.Open();

            using var beginCmd = connection.CreateCommand();
            beginCmd.CommandText = "BEGIN TRANSACTION";
            beginCmd.ExecuteNonQuery();

            using var insertCmd1 = connection.CreateCommand();
            insertCmd1.CommandText = $"INSERT INTO {TestTable} VALUES (33, 'first_sql_sync')";
            insertCmd1.ExecuteNonQuery();

            using var insertCmd2 = connection.CreateCommand();
            insertCmd2.CommandText = $"INSERT INTO {TestTable} VALUES (34, 'second_sql_sync')";
            insertCmd2.ExecuteNonQuery();

            using var connection2 = new FireboltConnection(ConnectionString());
            connection2.Open();
            using var selectCmd = connection2.CreateCommand();
            selectCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id IN (33, 34)";
            var countBeforeCommit = selectCmd.ExecuteScalar();
            Assert.That(countBeforeCommit, Is.EqualTo(0L));

            using var commitCmd = connection.CreateCommand();
            commitCmd.CommandText = "COMMIT";
            commitCmd.ExecuteNonQuery();

            using var selectAfterCmd = connection2.CreateCommand();
            selectAfterCmd.CommandText = $"SELECT COUNT(*) FROM {TestTable} WHERE id IN (33, 34)";
            var countAfterCommit = selectAfterCmd.ExecuteScalar();
            Assert.That(countAfterCommit, Is.EqualTo(2L));
        }

        [Test]
        public void TestNestedTransactionThrowsSqlSync()
        {
            using var connection = new FireboltConnection(ConnectionString());
            connection.Open();

            using var beginCmd1 = connection.CreateCommand();
            beginCmd1.CommandText = "BEGIN TRANSACTION";
            beginCmd1.ExecuteNonQuery();

            Assert.Throws<FireboltStructuredException>(() =>
            {
                using var beginCmd2 = connection.CreateCommand();
                beginCmd2.CommandText = "BEGIN TRANSACTION";
                beginCmd2.ExecuteNonQuery();
            });
        }
    }
}