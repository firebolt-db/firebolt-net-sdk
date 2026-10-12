using FireboltDotNetSdk.Client;
using FireboltNETSDK.Exception;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    internal class StreamingQueryTest : IntegrationTest
    {
        private static readonly string UserConnectionString = ConnectionString();

        [Test]
        [Category("engine-v2")]
        [Category("slow")]
        public async Task ExecuteStreamedQueryAsyncAndValidateSum()
        {
            await using var connection = new FireboltConnection(UserConnectionString);
            await connection.OpenAsync();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "select 1 from generate_series(1, 2500000)";

            await using var reader = await command.ExecuteStreamedQueryAsync();
            var sum = 0L;
            while (await reader.ReadAsync())
            {
                sum += reader.GetInt64(0);
            }

            Assert.That(sum, Is.EqualTo(2500000));
        }

        [Test]
        [Category("engine-v2")]
        public async Task ExecuteStreamedQueryAsyncWithSyntaxError()
        {
            await using var connection = new FireboltConnection(UserConnectionString);
            await connection.OpenAsync();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "select *1;";

            var exception = Assert.ThrowsAsync<FireboltStructuredException>(() => command.ExecuteStreamedQueryAsync());
            Assert.That(exception, Is.Not.Null);
            Assert.That(exception!.Message, Does.Contain("Line 1, Column 9: syntax error, unexpected integer, expecting end of file"));
            Assert.That(exception!.Message, Does.Contain("select *1;"));
        }

        [Test]
        [Category("engine-v2")]
        public async Task ExecuteStreamedQueryAsyncWithDivisionByZeroError()
        {
            await using var connection = new FireboltConnection(UserConnectionString);
            await connection.OpenAsync();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "select 1/(i-100000) as a from generate_series(1,100000) as i";
            var oneReadAtLeast = false;
            var exceptionThrown = false;

            await using var reader = await command.ExecuteStreamedQueryAsync();
            try
            {
                while (await reader.ReadAsync())
                {
                    oneReadAtLeast = true;
                }
            }
            catch (System.Exception e)
            {
                exceptionThrown = true;
                Assert.That(e, Is.Not.Null);
                Assert.That(e, Is.TypeOf(typeof(FireboltStructuredException)));
                Assert.That(e.Message, Does.StartWith("Line 1, Column 9: Division by zero\n" +
                                                               "select 1/(i-100000) as a from generate_series(1,..."));
                Assert.That(e.Message, Does.Contain(" at FailingLine: 1, StartOffset: 9"));
            }
            Assert.Multiple(() =>
            {
                Assert.That(oneReadAtLeast, Is.True, "Expected at least one row to be read before the exception was thrown.");
                Assert.That(exceptionThrown, Is.True, "Expected an exception to be thrown.");
            });
        }

        [Test]
        [Category("engine-v2")]
        [Category("slow")]
        public async Task ExecuteStreamedQueryAsync_LargeResult_IsNotBufferedInMemory()
        {
            const int rowCount = 1_000_000;
            await using var connection = new FireboltConnection(UserConnectionString);
            await connection.OpenAsync();

            var command = (FireboltCommand)connection.CreateCommand();
            // About 220 MB of JSON; buffering it as a string would take roughly three times that on the heap.
            command.CommandText = $"select i, '{new string('x', 200)}' as payload from generate_series(1, {rowCount}) as i";

            var baseline = GC.GetTotalMemory(forceFullCollection: true);
            long peak = 0;
            var rows = 0;
            await using (var reader = await command.ExecuteStreamedQueryAsync())
            {
                while (await reader.ReadAsync())
                {
                    if (rows++ % 100_000 == 0)
                        peak = Math.Max(peak, GC.GetTotalMemory(forceFullCollection: true) - baseline);
                }
            }

            Assert.Multiple(() =>
            {
                Assert.That(rows, Is.EqualTo(rowCount));
                Assert.That(peak, Is.LessThan(100L * 1024 * 1024), $"Managed heap grew by {peak / 1024 / 1024} MB while streaming");
            });
        }

        [Test]
        [Category("engine-v2")]
        public async Task ExecuteStreamedQueryAsync_CancelledMidStream_StopsReading()
        {
            await using var connection = new FireboltConnection(UserConnectionString);
            await connection.OpenAsync();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "select i from generate_series(1, 50000000) as i";
            using var cancellation = new CancellationTokenSource();

            await using var reader = await command.ExecuteStreamedQueryAsync(cancellation.Token);
            Assert.That(await reader.ReadAsync(), Is.True);
            cancellation.Cancel();

            var rowsAfterCancel = 0;
            Assert.Multiple(() =>
            {
                Assert.CatchAsync<OperationCanceledException>(async () =>
                {
                    while (await reader.ReadAsync()) rowsAfterCancel++;
                });
                Assert.That(rowsAfterCancel, Is.LessThan(50000000 - 1), "Reading must stop well before the end of the result");
            });
        }

        [Test]
        [Category("engine-v2")]
        public async Task ExecuteStreamedQueryAsyncWithPreparedStatement()
        {
            await using var connection = new FireboltConnection(UserConnectionString);
            await connection.OpenAsync();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "select 1 from generate_series(1, @max)";
            command.Parameters.Add(CreateParameter(command, "@max", 2));

            await using var reader = await command.ExecuteStreamedQueryAsync();
            Assert.Multiple(() =>
            {
                Assert.That(reader.Read(), Is.EqualTo(true));
                Assert.That(reader.GetInt32(0), Is.EqualTo(1));
                Assert.That(reader.Read(), Is.EqualTo(true));
                Assert.That(reader.GetInt32(0), Is.EqualTo(1));
                Assert.That(reader.Read(), Is.EqualTo(false), "Expected no more rows to be read after the second row.");
            });
        }

        [Test]
        [Category("engine-v2")]
        [Category("slow")]
        public void ExecuteStreamedQueryAndValidateSum()
        {
            using var connection = new FireboltConnection(UserConnectionString);
            connection.Open();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "select 1 from generate_series(1, 2500000)";

            using var reader = command.ExecuteStreamedQuery();
            var sum = 0L;
            while (reader.Read())
            {
                sum += reader.GetInt64(0);
            }

            Assert.That(sum, Is.EqualTo(2500000));
        }

        [Test]
        [Category("engine-v2")]
        public void ExecuteStreamedQueryWithSyntaxError()
        {
            using var connection = new FireboltConnection(UserConnectionString);
            connection.Open();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "select *1;";

            var exception = Assert.Throws<FireboltStructuredException>(() => command.ExecuteStreamedQuery());
            Assert.That(exception, Is.Not.Null);
            Assert.That(exception!.Message, Does.Contain("Line 1, Column 9: syntax error, unexpected integer, expecting end of file"));
            Assert.That(exception!.Message, Does.Contain("select *1;"));
        }

        [Test]
        [Category("engine-v2")]
        public void ExecuteStreamedQueryWithDivisionByZeroError()
        {
            using var connection = new FireboltConnection(UserConnectionString);
            connection.Open();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "select 1/(i-100000) as a from generate_series(1,100000) as i";
            var oneReadAtLeast = false;
            var exceptionThrown = false;

            using var reader = command.ExecuteStreamedQuery();
            try
            {
                while (reader.Read())
                {
                    oneReadAtLeast = true;
                }
            }
            catch (System.Exception e)
            {
                exceptionThrown = true;
                Assert.That(e, Is.Not.Null);
                Assert.That(e, Is.TypeOf(typeof(FireboltStructuredException)));
                Assert.That(e.Message, Does.StartWith("Line 1, Column 9: Division by zero\n" +
                                                               "select 1/(i-100000) as a from generate_series(1,..."));
                Assert.That(e.Message, Does.Contain(" at FailingLine: 1, StartOffset: 9"));
            }
            Assert.Multiple(() =>
            {
                Assert.That(oneReadAtLeast, Is.True, "Expected at least one row to be read before the exception was thrown.");
                Assert.That(exceptionThrown, Is.True, "Expected an exception to be thrown.");
            });
        }

        [Test]
        [Category("engine-v2")]
        public void ExecuteStreamedQueryWithPreparedStatement()
        {
            using var connection = new FireboltConnection(UserConnectionString);
            connection.Open();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "select 1 from generate_series(1, @max)";
            command.Parameters.Add(CreateParameter(command, "@max", 2));

            using var reader = command.ExecuteStreamedQuery();
            Assert.Multiple(() =>
            {
                Assert.That(reader.Read(), Is.EqualTo(true));
                Assert.That(reader.GetInt32(0), Is.EqualTo(1));
                Assert.That(reader.Read(), Is.EqualTo(true));
                Assert.That(reader.GetInt32(0), Is.EqualTo(1));
                Assert.That(reader.Read(), Is.EqualTo(false), "Expected no more rows to be read after the second row.");
            });
        }
    }
}
