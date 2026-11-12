using System.Data.Common;
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;

namespace FireboltDotNetSdk.Tests
{
    // Every value here travels as a URL query parameter and contains characters that change its meaning
    // when not encoded: '&' and '=' split it, '#' truncates it, '+' becomes a space and '%25' becomes '%'.
    [TestFixture]
    internal class QueryParameterEncodingTest : IntegrationTest
    {
        private static readonly int Suffix = new Random().Next(9999999);

        // Etc/GMT+1 is UTC-1; an unencoded '+' reaches the server as a space and names no valid time zone.
        [TestCase("SET time_zone=Etc/GMT+1")]
        [TestCase("SET time_zone = 'Etc/GMT+1'")]
        [TestCase("set time_zone='Etc/GMT+1';")]
        [Category("engine-v2")]
        public void SetTimeZoneWithPlusAndQuotesIsApplied(string setCommand)
        {
            using var connection = OpenConnection(PreparedStatementParamStyleType.Native);
            Execute(connection, setCommand);

            Assert.That(Scalar(connection, "SELECT '2024-01-01 12:00:00 UTC'::timestamptz::text"), Is.EqualTo("2024-01-01 11:00:00-01"));
            Assert.That(connection.SetParamList, Is.EqualTo(new HashSet<string> { "time_zone=Etc/GMT+1" }));
        }

        [TestCase("lbl&database=x #+y%25 100% ?/ü")]
        [TestCase("it's quoted")]
        [Category("engine-v2")]
        public void SetQueryLabelReachesTheServerUnchanged(string label)
        {
            using var connection = OpenConnection(PreparedStatementParamStyleType.Native);
            Execute(connection, $"SET query_label = '{label.Replace("'", "''")}'");

            // current_setting('query_label') is not supported in the cloud; the query finds its own running entry instead.
            var marker = Guid.NewGuid().ToString();
            Assert.That(Scalar(connection, $"SELECT query_label FROM information_schema.engine_running_queries WHERE query_text LIKE '%{marker}%'"),
                Is.EqualTo(label));
            Assert.That(Scalar(connection, "SELECT current_database()"), Is.EqualTo(connection.Database));
        }

        [Test]
        [Category("engine-v2")]
        public void UseDatabaseWithSpecialCharacters()
        {
            var databaseName = SpecialDatabaseName("use");
            using var connection = OpenConnection(PreparedStatementParamStyleType.Native);
            try
            {
                Execute(connection, $"CREATE DATABASE IF NOT EXISTS {Quote(databaseName)}");
                Execute(connection, $"USE DATABASE {Quote(databaseName)}");

                Assert.That(connection.Database, Is.EqualTo(databaseName));
                Assert.That(Scalar(connection, "SELECT current_database()"), Is.EqualTo(databaseName));
            }
            finally
            {
                DropDatabase(databaseName);
            }
        }

        [Test]
        [Category("engine-v2")]
        public void ConnectToDatabaseWithSpecialCharacters()
        {
            var databaseName = SpecialDatabaseName("connect");
            using (var setup = OpenConnection(PreparedStatementParamStyleType.Native))
            {
                Execute(setup, $"CREATE DATABASE IF NOT EXISTS {Quote(databaseName)}");
            }
            try
            {
                using var connection = OpenConnection(PreparedStatementParamStyleType.Native, databaseName);

                Assert.That(Scalar(connection, "SELECT current_database()"), Is.EqualTo(databaseName));
            }
            finally
            {
                DropDatabase(databaseName);
            }
        }

        private static string SpecialDatabaseName(string purpose)
        {
            return $"DotNet {purpose}&x=y #+z%25 ü_{Suffix}";
        }

        private static string Quote(string identifier)
        {
            return "\"" + identifier.Replace("\"", "\"\"") + "\"";
        }

        private static void DropDatabase(string databaseName)
        {
            try
            {
                using var connection = OpenConnection(PreparedStatementParamStyleType.Native);
                Execute(connection, $"DROP DATABASE IF EXISTS {Quote(databaseName)}");
            }
            catch (FireboltException e)
            {
                TestContext.Progress.WriteLine($"Could not drop database {databaseName}: {e.Message}");
            }
        }

        private static void Execute(DbConnection connection, string sql)
        {
            var command = connection.CreateCommand();
            command.CommandText = sql;
            command.ExecuteNonQuery();
        }

        private static object? Scalar(DbConnection connection, string sql)
        {
            var command = connection.CreateCommand();
            command.CommandText = sql;
            return command.ExecuteScalar();
        }
    }
}
