using FireboltDotNetSdk.Client;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    [Category("Integration")]
    [Category("FireboltCore")]
    public class CoreConnectionTest
    {
        [Test]
        public void ExecuteQueryAgainstFireboltCore()
        {
            var coreUrl = Environment.GetEnvironmentVariable("FIREBOLT_CORE_URL") ?? "http://localhost:3473";
            var database = Environment.GetEnvironmentVariable("FIREBOLT_CORE_DATABASE");
            var connectionString = $"url={coreUrl};ssl_mode=none";
            if (!string.IsNullOrEmpty(database))
            {
                connectionString += $";database={database}";
            }

            using var connection = new FireboltConnection(connectionString);
            connection.Open();

            using var command = connection.CreateCommand();
            command.CommandText = "SELECT 42";
            Assert.That(Convert.ToInt32(command.ExecuteScalar()), Is.EqualTo(42));
        }
    }
}
