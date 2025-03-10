using System.Data.Common;
using FireboltDotNetSdk.Client;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    internal class AsyncQueryTest : IntegrationTest
    {
        private static string USER_CONNECTION_STRING = ConnectionString();

        [Test]
        [Category("engine-v2")]
        public async Task ExecuteAsyncNonQueryTest()
        {
            using var conn = new FireboltConnection(USER_CONNECTION_STRING);
            conn.Open();

            try
            {
                // Create command to create a table
                FireboltCommand createTableCommand = (FireboltCommand)conn.CreateCommand();
                createTableCommand.CommandText = "CREATE TABLE IF NOT EXISTS async_test_table (id bigint)";
                await createTableCommand.ExecuteNonQueryAsync();

                // Create command with a computationally intensive query
                FireboltCommand command = (FireboltCommand)conn.CreateCommand();
                command.CommandText = "INSERT INTO async_test_table SELECT checksum(*) FROM GENERATE_SERIES(1, 2500000000)";

                // Execute the query asynchronously
                await command.ExecuteAsyncNonQueryAsync();

                // Retrieve the token from the command
                string? token = command.AsyncToken;

                // Verify we received a token
                Assert.That(token, Is.Not.Null);
                Assert.That(string.IsNullOrEmpty(token), Is.False);

                // Check the token format (should be non-empty string)
                Assert.That(token.Length, Is.GreaterThan(0));

                // Check that the query status is initially running
                Assert.That(await conn.IsAsyncQueryRunningAsync(token), Is.True);

                // Wait a bit for the query to make progress
                await Task.Delay(5000);

                // Check the status again - it should finished
                Assert.That(await conn.IsAsyncQueryRunningAsync(token), Is.False);
                Assert.That(await conn.IsAsyncQuerySuccessfulAsync(token), Is.True);

                // Verify the data was written to the table
                DbCommand countCommand = conn.CreateCommand();
                countCommand.CommandText = "SELECT COUNT(*) FROM async_test_table";
                var count = await countCommand.ExecuteScalarAsync();
                Assert.That(count, Is.EqualTo(1));
            }
            finally
            {
                // drop test table - will run even if previous commands fail
                DbCommand dropTableCommand = conn.CreateCommand();
                dropTableCommand.CommandText = "DROP TABLE IF EXISTS async_test_table";
                await dropTableCommand.ExecuteNonQueryAsync();
            }
        }

        [Test]
        [Category("engine-v2")]
        public void ExecuteAsyncNonQuerySyncTest()
        {
            using var conn = new FireboltConnection(USER_CONNECTION_STRING);
            conn.Open();

            try
            {
                // Create command to create a table
                FireboltCommand createTableCommand = (FireboltCommand)conn.CreateCommand();
                createTableCommand.CommandText = "CREATE TABLE IF NOT EXISTS async_test_table_sync (id bigint)";
                createTableCommand.ExecuteNonQuery();

                // Create command with a computationally intensive query
                FireboltCommand command = (FireboltCommand)conn.CreateCommand();
                command.CommandText = "INSERT INTO async_test_table_sync SELECT checksum(*) FROM GENERATE_SERIES(1, 2500000000)";

                // Execute the query asynchronously using the synchronous method
                command.ExecuteAsyncNonQuery();

                // Retrieve the token from the command
                string? token = command.AsyncToken;

                // Verify we received a token
                Assert.That(token, Is.Not.Null);
                Assert.That(string.IsNullOrEmpty(token), Is.False);

                // Check the token format (should be non-empty string)
                Assert.That(token.Length, Is.GreaterThan(0));

                // Check that the query status is initially running
                Assert.That(conn.IsAsyncQueryRunning(token), Is.True);

                // Wait a bit for the query to make progress
                Thread.Sleep(5000);

                // Check the status again - it should finished
                Assert.That(conn.IsAsyncQueryRunning(token), Is.False);
                Assert.That(conn.IsAsyncQuerySuccessful(token), Is.True);

                // Verify the data was written to the table
                DbCommand countCommand = conn.CreateCommand();
                countCommand.CommandText = "SELECT COUNT(*) FROM async_test_table_sync";
                var count = countCommand.ExecuteScalar();
                Assert.That(count, Is.EqualTo(1));
            }
            finally
            {
                // drop test table - will run even if previous commands fail
                DbCommand dropTableCommand = conn.CreateCommand();
                dropTableCommand.CommandText = "DROP TABLE IF EXISTS async_test_table_sync";
                dropTableCommand.ExecuteNonQuery();
            }
        }

        [Test]
        [Category("engine-v2")]
        public async Task CancelAsyncQueryTest()
        {
            using var conn = new FireboltConnection(USER_CONNECTION_STRING);
            conn.Open();

            try
            {
                // Create command to create a table
                FireboltCommand createTableCommand = (FireboltCommand)conn.CreateCommand();
                createTableCommand.CommandText = "CREATE TABLE IF NOT EXISTS async_test_table_cancel (id bigint)";
                await createTableCommand.ExecuteNonQueryAsync();

                // Create command with a computationally intensive query that will take a while
                FireboltCommand command = (FireboltCommand)conn.CreateCommand();
                command.CommandText = "INSERT INTO async_test_table_cancel SELECT checksum(*) FROM GENERATE_SERIES(1, 5000000000)";

                // Execute the query asynchronously
                await command.ExecuteAsyncNonQueryAsync();
                string? token = command.AsyncToken;
                Assert.That(token, Is.Not.Null);

                // Wait a moment to make sure the query starts running
                await Task.Delay(1000);

                // Verify the query is running
                bool isRunning = await conn.IsAsyncQueryRunningAsync(token);
                Assert.That(isRunning, Is.True, "Query should be running before cancellation");

                // Stop the query
                bool stopped = await conn.CancelAsyncQueryAsync(token);
                Assert.That(stopped, Is.True, "Failed to stop the async query");

                // Verify the query is no longer running
                isRunning = await conn.IsAsyncQueryRunningAsync(token);
                Assert.That(isRunning, Is.False, "Query should no longer be running");

                // Verify no data was written to the table
                DbCommand countCommand = conn.CreateCommand();
                countCommand.CommandText = "SELECT COUNT(*) FROM async_test_table_cancel";
                var count = await countCommand.ExecuteScalarAsync();
                Assert.That(count, Is.EqualTo(0));
            }
            finally
            {
                // drop test table - will run even if previous commands fail
                DbCommand dropTableCommand = conn.CreateCommand();
                dropTableCommand.CommandText = "DROP TABLE IF EXISTS async_test_table_cancel";
                await dropTableCommand.ExecuteNonQueryAsync();
            }
        }
    }
}
