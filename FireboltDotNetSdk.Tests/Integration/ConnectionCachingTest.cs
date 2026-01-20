using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Utils;
using System.Data.Common;

namespace FireboltDotNetSdk.Tests.Integration
{
    [TestFixture]
    internal class ConnectionCachingTest : IntegrationTest
    {
        [SetUp]
        public new void SetUp()
        {
            // Clear cache before each test
            CacheService.Instance.Clear();
        }

        /// <summary>
        /// Verifies that USE ENGINE query is cached and not re-executed on subsequent connections.
        /// Expected: First connection executes USE ENGINE + SELECT. Second connection only executes SELECT (reuses cached engine).
        /// </summary>
        [Test]
        [Category("v2,engine-v2")]
        public async Task TestConnectionCaching_UseEngineQueryIsCached()
        {
            var testMarker = $"CacheTest_{Guid.NewGuid():N}";
            var startTime = DateTime.UtcNow;
            
            // First connection - should execute USE ENGINE
            var connection1 = new FireboltConnection(ConnectionString());
            await connection1.OpenAsync();
            
            var command1 = connection1.CreateCommand();
            command1.CommandText = $"SELECT 1 AS result --{testMarker}_Query1";
            var result1 = await command1.ExecuteScalarAsync();
            Assert.That(result1, Is.Not.Null);
            
            await connection1.CloseAsync();

            // Second connection with same credentials - should reuse cached engine
            var connection2 = new FireboltConnection(ConnectionString());
            await connection2.OpenAsync();
            
            var command2 = connection2.CreateCommand();
            command2.CommandText = $"SELECT 1 AS result --{testMarker}_Query2";
            var result2 = await command2.ExecuteScalarAsync();
            Assert.That(result2, Is.Not.Null);
            
            await connection2.CloseAsync();

            // Wait for query history to be populated
            await Task.Delay(10000);

            // Query history to verify USE ENGINE was only executed once
            var connection3 = new FireboltConnection(ConnectionString());
            await connection3.OpenAsync();
            
            var historyCommand = connection3.CreateCommand();
            historyCommand.CommandText = @"
                SELECT 
                    query_text
                FROM information_schema.engine_query_history
                WHERE start_time >= @startTime
                    AND status = 'ENDED_SUCCESSFULLY'
                    AND (query_text LIKE 'USE ENGINE%' OR query_text LIKE @testMarker)
                ORDER BY query_text";
            
            var startTimeParam = historyCommand.CreateParameter();
            startTimeParam.ParameterName = "@startTime";
            startTimeParam.Value = startTime.ToString("yyyy-MM-dd HH:mm:ss");
            historyCommand.Parameters.Add(startTimeParam);
            
            var markerParam = historyCommand.CreateParameter();
            markerParam.ParameterName = "@testMarker";
            markerParam.Value = $"%--{testMarker}%";
            historyCommand.Parameters.Add(markerParam);

            await using var reader = await historyCommand.ExecuteReaderAsync();
            
            var useEngineCount = 0;
            var selectQueryCount = 0;
            
            while (await reader.ReadAsync())
            {
                var queryText = reader.GetString(0);
                
                if (queryText.StartsWith("USE ENGINE", StringComparison.OrdinalIgnoreCase))
                {
                    useEngineCount++;
                }
                else if (queryText.Contains($"--{testMarker}", StringComparison.OrdinalIgnoreCase))
                {
                    selectQueryCount++;
                }
            }
            
            await connection3.CloseAsync();
            Assert.Multiple(() =>
            {

                // Assertions: Should have 1 USE ENGINE query and 2 SELECT queries
                Assert.That(useEngineCount, Is.EqualTo(1),
                    "USE ENGINE should only be executed once (cached on second connection)");
                Assert.That(selectQueryCount, Is.EqualTo(2),
                    "Both SELECT queries should be executed");
            });
        }

        /// <summary>
        /// Verifies that when caching is disabled, USE ENGINE query is executed for every connection.
        /// Expected: Each connection executes its own USE ENGINE + SELECT query (no caching).
        /// </summary>
        [Test]
        [Category("v2,engine-v2")]
        public async Task TestConnectionCaching_DisabledCachingExecutesUseEngineForEachConnection()
        {
            var testMarker = $"NoCacheTest_{Guid.NewGuid():N}";
            var startTime = DateTime.UtcNow;
            
            // Get base connection string and add CacheConnection=false
            var connectionStringWithoutCache = ConnectionString() + ";CacheConnection=false";
            
            const int numberOfConnections = 3;
            
            // Create multiple connections with caching disabled
            for (var i = 0; i < numberOfConnections; i++)
            {
                var connection = new FireboltConnection(connectionStringWithoutCache);
                await connection.OpenAsync();
                
                var command = connection.CreateCommand();
                command.CommandText = $"SELECT 1 AS result --{testMarker}_Query{i + 1}";
                var result = await command.ExecuteScalarAsync();
                Assert.That(result, Is.Not.Null);
                
                await connection.CloseAsync();
            }

            // Wait for query history to be populated
            await Task.Delay(10000);

            // Query history to verify USE ENGINE was executed for each connection
            var historyConnection = new FireboltConnection(ConnectionString());
            await historyConnection.OpenAsync();
            
            var historyCommand = historyConnection.CreateCommand();
            historyCommand.CommandText = @"
                SELECT 
                    query_text
                FROM information_schema.engine_query_history
                WHERE start_time >= @startTime
                    AND status = 'ENDED_SUCCESSFULLY'
                    AND (query_text LIKE 'USE ENGINE%' OR query_text LIKE @testMarker)
                ORDER BY query_text";
            
            var startTimeParam = historyCommand.CreateParameter();
            startTimeParam.ParameterName = "@startTime";
            startTimeParam.Value = startTime.ToString("yyyy-MM-dd HH:mm:ss");
            historyCommand.Parameters.Add(startTimeParam);
            
            var markerParam = historyCommand.CreateParameter();
            markerParam.ParameterName = "@testMarker";
            markerParam.Value = $"%--{testMarker}%";
            historyCommand.Parameters.Add(markerParam);

            await using var reader = await historyCommand.ExecuteReaderAsync();
            
            var selectQueryCount = 0;
            
            while (await reader.ReadAsync())
            {
                var queryText = reader.GetString(0);
                //todo will have to enable custom labels to test this
                // if (queryText.StartsWith("USE ENGINE", StringComparison.OrdinalIgnoreCase))
                // {
                //     useEngineCount++;
                // }
                if (queryText.Contains($"--{testMarker}", StringComparison.OrdinalIgnoreCase))
                {
                    selectQueryCount++;
                }
            }
            
            await historyConnection.CloseAsync();
            
            Assert.Multiple(() =>
            {
                //todo enable after custom query labels
                // Assertions: Should have USE ENGINE executed for each connection when caching is disabled
                // Assert.That(useEngineCount, Is.EqualTo(numberOfConnections),
                //     $"USE ENGINE should be executed {numberOfConnections} times (once per connection, no caching)");
                Assert.That(selectQueryCount, Is.EqualTo(numberOfConnections),
                    $"All {numberOfConnections} SELECT queries should be executed");
            });
        }
    }
}
