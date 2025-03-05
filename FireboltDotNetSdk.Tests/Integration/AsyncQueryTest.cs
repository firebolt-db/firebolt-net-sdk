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
        public async Task ExecuteAsyncQueryTest()
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
                string token = await command.ExecuteAsyncQueryAsync();

                // Verify we received a token
                Assert.NotNull(token);
                Assert.IsFalse(string.IsNullOrEmpty(token));

                // Verify the token is stored in the command
                Assert.That(command.AsyncToken, Is.EqualTo(token));

                // Check the token format (should be non-empty string)
                Assert.That(token.Length, Is.GreaterThan(0));

                // Check that the query status is initially RUNNING or QUEUED
                var initialStatusInfo = await conn.GetAsyncQueryStatusAsync(token);
                Assert.That(initialStatusInfo.ContainsKey("status"), Is.True);
                Assert.That(initialStatusInfo["status"], Is.EqualTo("RUNNING"));
                Assert.That(await conn.IsAsyncQueryRunningAsync(token), Is.True);

                // Wait a bit for the query to make progress
                await Task.Delay(5000);

                // Check the status again - still should be running or possibly finished
                var laterStatusInfo = await conn.GetAsyncQueryStatusAsync(token);
                Assert.That(laterStatusInfo.ContainsKey("status"), Is.True);
                Assert.That(laterStatusInfo["status"], Is.EqualTo("ENDED_SUCCESSFULLY"));

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
        public void ExecuteAsyncQuerySyncTest()
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
                string token = command.ExecuteAsyncQuery();

                // Verify we received a token
                Assert.NotNull(token);
                Assert.IsFalse(string.IsNullOrEmpty(token));

                // Verify the token is stored in the command
                Assert.That(command.AsyncToken, Is.EqualTo(token));

                // Check the token format (should be non-empty string)
                Assert.That(token.Length, Is.GreaterThan(0));

                // Check that the query status is initially RUNNING or QUEUED
                var initialStatusInfo = conn.GetAsyncQueryStatus(token);
                Assert.That(initialStatusInfo.ContainsKey("status"), Is.True);
                Assert.That(initialStatusInfo["status"], Is.EqualTo("RUNNING"));
                Assert.That(conn.IsAsyncQueryRunning(token), Is.True);

                // Wait a bit for the query to make progress
                Thread.Sleep(5000);

                // Check the status again - still should be running or possibly finished
                var laterStatusInfo = conn.GetAsyncQueryStatus(token);
                Assert.That(laterStatusInfo.ContainsKey("status"), Is.True);
                Assert.That(laterStatusInfo["status"], Is.EqualTo("ENDED_SUCCESSFULLY"));
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
                string token = await command.ExecuteAsyncQueryAsync();
                Assert.NotNull(token);
                
                // Wait a moment to make sure the query starts running
                await Task.Delay(1000);
                
                // Verify the query is running
                var initialStatus = await conn.GetAsyncQueryStatusAsync(token);
                Assert.That(initialStatus.ContainsKey("status"), Is.True);
                Assert.That(initialStatus["status"], Is.EqualTo("RUNNING"));
                
                // Stop the query
                bool stopped = await conn.CancelAsyncQueryAsync(token);
                Assert.That(stopped, Is.True, "Failed to stop the async query");
                
                // Verify the query is no longer running
                var updatedStatus = await conn.GetAsyncQueryStatusAsync(token);
                Assert.That(updatedStatus.ContainsKey("status"), Is.True);
                Assert.That(updatedStatus["status"], Is.Not.EqualTo("RUNNING"));
                
                // Verify using the helper method
                bool running = await conn.IsAsyncQueryRunningAsync(token);
                Assert.That(running, Is.False, "Query should no longer be running");

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
