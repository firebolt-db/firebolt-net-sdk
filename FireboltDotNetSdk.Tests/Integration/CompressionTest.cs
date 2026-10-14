using FireboltDotNetSdk.Client;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    internal class CompressionTest : IntegrationTest
    {
        [TestCase(true)]
        [TestCase(false)]
        [Category("general")]
        public async Task Query_WithAndWithoutCompression_ReturnsTheResult(bool compression)
        {
            await using var connection = new FireboltConnection(ConnectionString() + $";compression={compression}");
            await connection.OpenAsync();

            var command = connection.CreateCommand();
            command.CommandText = "select sum(i) from generate_series(1, 100000) as i";
            Assert.That(Convert.ToInt64(await command.ExecuteScalarAsync()), Is.EqualTo(5000050000L));
        }

        [TestCase(true)]
        [TestCase(false)]
        [Category("engine-v2")]
        public async Task StreamedQuery_WithAndWithoutCompression_ReturnsEveryRow(bool compression)
        {
            await using var connection = new FireboltConnection(ConnectionString() + $";compression={compression}");
            await connection.OpenAsync();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "select i from generate_series(1, 100000) as i";
            await using var reader = await command.ExecuteStreamedQueryAsync();
            long sum = 0;
            while (await reader.ReadAsync()) sum += reader.GetInt64(0);
            Assert.That(sum, Is.EqualTo(5000050000L));
        }
    }
}
