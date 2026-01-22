using System.Data.Common;
using FireboltDotNetSdk.Client;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    internal class AsyncQueryTest : IntegrationTest
    {
        private static string USER_CONNECTION_STRING = ConnectionString();
        private string _tableName;

        [SetUp]
        public new void SetUp()
        {
            base.SetUp();

            // Generate a unique table name for each test
            _tableName = $"async_test_table_{Guid.NewGuid().ToString("N").Substring(0, 8)}";

            using var setupConnection = new FireboltConnection(USER_CONNECTION_STRING);
            setupConnection.Open();

            // Create test table
            FireboltCommand createTableCommand = (FireboltCommand)setupConnection.CreateCommand();
            createTableCommand.CommandText = $"CREATE TABLE IF NOT EXISTS {_tableName} (id bigint)";
            createTableCommand.ExecuteNonQuery();
        }

        [TearDown]
        public void TearDown()
        {
            using var teardownConnection = new FireboltConnection(USER_CONNECTION_STRING);
            teardownConnection.Open();

            // Drop test table
            DbCommand dropTableCommand = teardownConnection.CreateCommand();
            dropTableCommand.CommandText = $"DROP TABLE IF EXISTS {_tableName}";
            dropTableCommand.ExecuteNonQuery();
        }

        [Test]
        [Category("engine-v2")]
        [Category("slow")]
        public async Task ExecuteServerSideAsyncNonQueryTest()
        {
            using var connection = new FireboltConnection(USER_CONNECTION_STRING);
            await connection.OpenAsync();

            // Create command with a computationally intensive query
            FireboltCommand command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = $"INSERT INTO {_tableName} SELECT checksum(*) FROM GENERATE_SERIES(1, 2500000000)";

            await command.ExecuteServerSideAsyncNonQueryAsync();

            string token = command.AsyncToken ?? string.Empty;
            Assert.Multiple(() =>
            {

                // Verify we received a token
                Assert.That(token, Is.Not.Null);
                Assert.That(string.IsNullOrEmpty(token), Is.False);
            });
            Assert.Multiple(async () =>
            {
                Assert.That(token, Is.Not.Empty);

                // Check that the query status is initially running
                Assert.That(await connection.IsServerSideAsyncQueryRunningAsync(token), Is.True);
            });

            // Wait a bit for the query to make progress
            await Task.Delay(5000);
            Assert.Multiple(async () =>
            {

                // Check the status again - it should be finished
                Assert.That(await connection.IsServerSideAsyncQueryRunningAsync(token), Is.False);
                Assert.That(await connection.IsServerSideAsyncQuerySuccessfulAsync(token), Is.True);
            });

            // Verify the data was written to the table
            DbCommand countCommand = connection.CreateCommand();
            countCommand.CommandText = $"SELECT COUNT(*) FROM {_tableName}";
            var count = await countCommand.ExecuteScalarAsync();
            Assert.That(count, Is.EqualTo(1));
        }

        [Test]
        [Category("engine-v2")]
        [Category("slow")]
        public void ExecuteServerSideAsyncNonQuerySyncTest()
        {
            using var connection = new FireboltConnection(USER_CONNECTION_STRING);
            connection.Open();

            // Create command with a computationally intensive query
            FireboltCommand command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = $"INSERT INTO {_tableName} SELECT checksum(*) FROM GENERATE_SERIES(1, 2500000000)";

            // Execute the query asynchronously using the synchronous method
            command.ExecuteServerSideAsyncNonQuery();

            string? token = command.AsyncToken;

            // Verify we received a token
            Assert.That(token, Is.Not.Null);
            Assert.Multiple(() =>
            {
                Assert.That(string.IsNullOrEmpty(token), Is.False);
                Assert.That(token, Is.Not.Empty);
            });

            // Check that the query status is initially running
            Assert.That(connection.IsServerSideAsyncQueryRunning(token), Is.True);

            // Wait a bit for the query to make progress
            Task.Delay(5000).Wait();

            // Check the status again - it should be finished
            Assert.Multiple(() =>
            {
                Assert.That(connection.IsServerSideAsyncQueryRunning(token), Is.False);
                Assert.That(connection.IsServerSideAsyncQuerySuccessful(token), Is.True);
            });

            // Verify the data was written to the table
            DbCommand countCommand = connection.CreateCommand();
            countCommand.CommandText = $"SELECT COUNT(*) FROM {_tableName}";
            var count = countCommand.ExecuteScalar();
            Assert.That(count, Is.EqualTo(1));
        }

        [Test]
        [Category("engine-v2")]
        public async Task CancelServerSideAsyncQueryTest()
        {
            using var connection = new FireboltConnection(USER_CONNECTION_STRING);
            await connection.OpenAsync();

            // Create command with a computationally intensive query that will take a while
            FireboltCommand command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = $"INSERT INTO {_tableName} SELECT checksum(*) FROM GENERATE_SERIES(1, 5000000000)";

            await command.ExecuteServerSideAsyncNonQueryAsync();
            string? token = command.AsyncToken;
            Assert.That(token, Is.Not.Null);

            // Wait a moment to make sure the query starts running
            await Task.Delay(1000);

            // Verify the query is running
            bool isRunning = await connection.IsServerSideAsyncQueryRunningAsync(token);
            Assert.That(isRunning, Is.True, "Query should be running before cancellation");

            // Stop the query
            bool stopped = await connection.CancelServerSideAsyncQueryAsync(token);
            Assert.That(stopped, Is.True, "Failed to stop the async query");

            // Verify the query is no longer running
            isRunning = await connection.IsServerSideAsyncQueryRunningAsync(token);
            Assert.That(isRunning, Is.False, "Query should no longer be running");

            // Verify no data was written to the table
            DbCommand countCommand = connection.CreateCommand();
            countCommand.CommandText = $"SELECT COUNT(*) FROM {_tableName}";
            var count = await countCommand.ExecuteScalarAsync();
            Assert.That(count, Is.EqualTo(0));
        }
    }
}
