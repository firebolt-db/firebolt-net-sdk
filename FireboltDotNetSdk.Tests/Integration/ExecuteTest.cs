using System.Text;
using System.Data.Common;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;

namespace FireboltDotNetSdk.Tests
{

    [TestFixture]
    internal class ExecutionTest : IntegrationTest
    {
        private static string SYSTEM_CONNECTION_STRING = $"database={Database};clientid={ClientId};clientsecret={ClientSecret};endpoint={Endpoint};account={Account};env={Env}";
        private static string USER_CONNECTION_STRING = $"{SYSTEM_CONNECTION_STRING};engine={EngineName}";
        private static string SYSTEM_WRONG_CREDENTIALS = $"database={Database};clientid={ClientId};clientsecret=wrongClientSecret;endpoint={Endpoint};account={Account};env={Env}";

        [TestCase("SELECT 1")]
        [TestCase("SELECT 1, 'a'")]
        [TestCase("SELECT 1 as uint8")]
        [TestCase("SELECT 257 as uint16")]
        [TestCase("SELECT -257 as int16")]
        [TestCase("SELECT 80000 as uint32")]
        [TestCase("SELECT -80000 as int32")]
        [TestCase("SELECT 30000000000 as uint64")]
        [TestCase("SELECT -30000000000 as int64")]
        public void ExecuteSelectTest(string commandText)
        {
            executeTest(SYSTEM_CONNECTION_STRING, commandText, null, true);
        }

        [Ignore("sleepEachRow function is currently not supported on staging")]
        [TestCase("select sleepEachRow(1) from numbers(5)")]
        public void ExecuteSetTest(string commandText)
        {
            executeTest(SYSTEM_CONNECTION_STRING, commandText, "SET use_standard_sql=0", true);
        }

        [TestCase("select * from information_schema.tables")]
        public void ExecuteSetEngineTest(string commandText)
        {
            executeTest(USER_CONNECTION_STRING, commandText, null, false);
        }

        private void executeTest(string connString, string commandText, string? additionalCommandText, bool noNextLine)
        {
            using var conn = new FireboltConnection(connString);
            conn.Open();
            if (additionalCommandText != null)
            {
                DbCommand addCommand = conn.CreateCommand();
                addCommand.CommandText = additionalCommandText;
                addCommand.ExecuteNonQuery();
            }
            DbCommand command = conn.CreateCommand();
            command.CommandText = commandText;
            DbDataReader reader = command.ExecuteReader();
            Assert.NotNull(reader);
            Assert.That(reader.Read(), Is.EqualTo(true));
            Assert.NotNull(reader.GetValue(0));
            if (noNextLine)
            {
                Assert.That(reader.Read(), Is.EqualTo(false));
            }
        }

        [Test]
        public void ExecuteTestInvalidCredentials()
        {
            using var conn = new FireboltConnection(SYSTEM_WRONG_CREDENTIALS);
            FireboltException? exception = Assert.Throws<FireboltException>(() => conn.Open());
            Assert.NotNull(exception);
            Assert.That(exception!.Message, Does.Contain("The operation is unauthorized\nStatus: 401"));
            Assert.That(exception.ToString(), Does.Contain("FireboltDotNetSdk.Exception.FireboltException: The operation is unauthorized\nStatus: 401"));
        }

        [Test]
        public void ExecuteSelectTimestampNtz()
        {
            ExecuteDateTimeTest(
                USER_CONNECTION_STRING,
                "SELECT '2022-05-10 23:01:02.123455'::timestampntz",
                new string[0],
                new DateTime(2022, 5, 10, 23, 1, 2, 0).AddTicks(1234550));
        }

        [Test]
        public void ExecuteSelectTimestampPgDate()
        {
            ExecuteDateTimeTest(
                USER_CONNECTION_STRING,
                "SELECT '2022-05-10'::pgdate",
                new string[0],
                new DateTime(2022, 5, 10, 0, 0, 0),
                new DateOnly(2022, 5, 10));
        }

        [Test]
        public void ExecuteSelectTimestampTz()
        {
            using var conn = new FireboltConnection(USER_CONNECTION_STRING);
            conn.Open();
            ExecuteDateTimeTest(
                USER_CONNECTION_STRING,
                "SELECT '2022-05-10 23:01:02.123456 Europe/Berlin'::timestamptz",
                new string[0],
                DateTime.Parse("2022-05-10 21:01:02.123456Z"));
        }

        [Test]
        public void ExecuteSelectTimestampTzWithMinutesInTz()
        {
            ExecuteDateTimeTest(
                USER_CONNECTION_STRING,
                "SELECT '2022-05-11 23:01:02.123123 Europe/Berlin'::timestamptz",
                new string[] { "SET advanced_mode=1", "SET time_zone=Asia/Calcutta" },
                DateTime.Parse("2022-05-11 21:01:02.123123Z"));
        }

        [Test]
        public void ExecuteSelectTimestampTzWithTzWithMinutesAndSecondsInTz()
        {
            ExecuteDateTimeTest(
                USER_CONNECTION_STRING,
                "SELECT '1111-01-05 17:04:42.123456'::timestamptz",
                new string[] { "SET advanced_mode=1", "SET time_zone=Asia/Calcutta" },
                DateTime.Parse("1111-01-05 11:11:14.123456Z"));
        }

        [Test]
        public void ExecuteSelectTimestampTzWithTzWithDefaultTz()
        {
            ExecuteDateTimeTest(
                USER_CONNECTION_STRING,
                "SELECT '2022-05-01 12:01:02.123456'::timestamptz",
                new string[0],
                DateTime.Parse("2022-05-01 12:01:02.123456Z"));
        }

        private void ExecuteDateTimeTest(string connString, string commandText, string[] additionalCommands, DateTime expectedDateTime)
        {
            ExecuteDateTimeTest(connString, commandText, additionalCommands, expectedDateTime, expectedDateTime);
        }

        private void ExecuteDateTimeTest(string connString, string commandText, string[] additionalCommands, DateTime expectedDateTime, object expectedValue)
        {
            using var conn = new FireboltConnection(connString);
            conn.Open();
            foreach (string cmd in additionalCommands)
            {
                DbCommand addCommand = conn.CreateCommand();
                addCommand.CommandText = cmd;
                addCommand.ExecuteNonQuery();
            }
            DbCommand command = conn.CreateCommand();
            command.CommandText = commandText;
            DbDataReader reader = command.ExecuteReader();
            Assert.NotNull(reader);
            Assert.That(reader.Read(), Is.EqualTo(true));
            Assert.That(reader.GetDateTime(0), Is.EqualTo(expectedDateTime));
            Assert.That(reader.GetValue(0), Is.EqualTo(expectedValue));
            Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(DateTime)));
            Assert.That(reader.Read(), Is.EqualTo(false));
            conn.Close();
        }


        [Test]
        public void ExecuteSelectBoolean()
        {
            using var conn = new FireboltConnection(USER_CONNECTION_STRING);
            conn.Open();

            DbCommand command = conn.CreateCommand();
            new List<string>()
            {
                "SET advanced_mode=1", "SET output_format_firebolt_type_names=true", "SET bool_output_format=postgres"
            }
            .ForEach(set => { command.CommandText = set; command.ExecuteNonQuery(); });
            command.CommandText = "SELECT true::boolean";
            DbDataReader reader = command.ExecuteReader();
            Assert.IsTrue(reader.Read());
            Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(bool)));
            Assert.That(reader.GetBoolean(0), Is.EqualTo(true));
            Assert.IsFalse(reader.Read());
        }

        [Test]
        public void ExecuteServiceAccountLogin()
        {
            using var conn = new FireboltConnection(SYSTEM_CONNECTION_STRING);
            conn.Open();
            DbCommand command = conn.CreateCommand();
            command.CommandText = "SELECT 1";
            DbDataReader reader = command.ExecuteReader();
            Assert.That(reader.Read(), Is.EqualTo(true));
            Assert.That(reader.GetInt16(0), Is.EqualTo(1));
            Assert.That(reader.Read(), Is.EqualTo(false));
            conn.Close();
        }

        [Test]
        public void ExecuteInformationSchema()
        {
            using var conn = new FireboltConnection(SYSTEM_CONNECTION_STRING);
            conn.Open();
            DbCommand command = conn.CreateCommand();
            command.CommandText = "SELECT table_name, number_of_rows from information_schema.tables";
            DbDataReader reader = command.ExecuteReader();
            List<string> tableNames = new List<string>();
            while (reader.Read())
            {
                string name = reader.GetString(0);
                tableNames.Add(name);
                long rowsCount = reader.GetInt64(1);
                Assert.That(rowsCount, Is.GreaterThanOrEqualTo(0));
            }
            List<string> expectedTables = new List<string>() { "databases", "engines", "tables", "views", "columns", "accounts", "users" };
            expectedTables.ForEach(table => Assert.That(tableNames.Contains(table), Is.EqualTo(true)));
        }


        [Test]
        public void ExecuteServiceAccountLoginWithInvalidCredentials()
        {
            using var conn = new FireboltConnection(SYSTEM_WRONG_CREDENTIALS);
            FireboltException? exception = Assert.Throws<FireboltException>(() => conn.Open());
            Assert.NotNull(exception);
            Assert.IsTrue(exception!.Message.Contains("401"));
        }

        [Test]
        public void ExecuteServiceAccountLoginWithMissingSecret()
        {
            var connString = $"database={Database};clientid={ClientId};clientsecret=;endpoint={Endpoint};account={Account};env={Env}";
            FireboltException? exception = Assert.Throws<FireboltException>(() => new FireboltConnection(connString));
            Assert.NotNull(exception);
            Assert.IsTrue(exception!.Message.Contains("ClientSecret parameter is missing in the connection string"));
        }

        [Test]
        public void ExecuteSelectArray()
        {
            ExecuteTest(SYSTEM_CONNECTION_STRING, "select [1,NULL,3]", new int?[] { 1, null, 3 }, typeof(int[]));
        }

        [Test]
        public void ExecuteSelectTwoDimensionalArray()
        {
            ExecuteTest(SYSTEM_CONNECTION_STRING, "select [[1,NULL,3]]", new int?[1][] { new int?[] { 1, null, 3 } }, typeof(int[][]));
        }

        private void ExecuteTest(string connString, string commandText, object expectedValue, Type expectedType)
        {
            using var conn = new FireboltConnection(connString);
            conn.Open();
            DbCommand command = conn.CreateCommand();
            command.CommandText = commandText;
            DbDataReader reader = command.ExecuteReader();
            Assert.NotNull(reader);
            Assert.That(reader.Read(), Is.EqualTo(true));
            Assert.That(reader.GetValue(0), Is.EqualTo(expectedValue));
            //Assert.That(reader.GetFieldType(0), Is.EqualTo(expectedType)); //TODO: improve type discovery and uncomment this line
            Assert.That(reader.Read(), Is.EqualTo(false));
            conn.Close();
        }

        [Test]
        public void ExecuteSelectByteA()
        {
            ExecuteTest(SYSTEM_CONNECTION_STRING, "SELECT 'hello_world_123ツ\n\u0048'::bytea", Encoding.UTF8.GetBytes("hello_world_123ツ\n\u0048"), typeof(byte[]));
        }

        [Test]
        public void ShouldThrowExceptionWhenEngineIsNotFound()
        {
            var connString = $"{SYSTEM_CONNECTION_STRING};engine=InexistantEngine";
            using var conn = new FireboltConnection(connString);
            FireboltException? exception = Assert.Throws<FireboltException>(() => conn.Open());
            Assert.That(exception!.Message, Is.EqualTo($"Engine InexistantEngine not found."));
        }

        [Test]
        public void SetEngine()
        {
            ExecuteTest(USER_CONNECTION_STRING, "SELECT 1", 1, typeof(int));
        }
    }
}
