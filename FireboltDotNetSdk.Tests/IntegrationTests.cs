using System;
using FireboltDotNetSdk.Client;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    [Category("Integration")]
    internal class IntegrationTests
    {
        private string _database;
        private string _username;
        private string _password;
        private string _endpoint;
        private string _account;
        private string _engine;

	public static string WithDefault(string val, string default_value) {
	    if (val != null) {
		return val;
	    }
	    return default_value;
	}

	[SetUp]
        public void Init()
        {
	    _database = WithDefault(Environment.GetEnvironmentVariable("FIREBOLT_DATABASE"), null);
	    _username = WithDefault(Environment.GetEnvironmentVariable("FIREBOLT_USERNAME"), null);
	    _password = WithDefault(Environment.GetEnvironmentVariable("FIREBOLT_PASSWORD"), null);
	    _endpoint = WithDefault(Environment.GetEnvironmentVariable("FIREBOLT_ENDPOINT"), "https://api.dev.firebolt.io");
	    _account = WithDefault(Environment.GetEnvironmentVariable("FIREBOLT_ACCOUNT"), "firebolt");
	    _engine = WithDefault(Environment.GetEnvironmentVariable("FIREBOLT_ENGINE_URL"), null);
        }

        [TestCase("SELECT 1")]
        [TestCase("SELECT 1, 'a'")]
        [TestCase("SELECT 1 as uint8")]
        [TestCase("SELECT 257 as uint16")]
        [TestCase("SELECT -257 as int16")]
        [TestCase("SELECT 80000 as uint32")]
        [TestCase("SELECT -80000 as int32")]
        [TestCase("SELECT 30000000000 as uint64")]
        [TestCase("SELECT -30000000000 as int64")] 
        public void ExecuteTest(string commandText)
        {
            var connString = $"database={_database};username={_username};password={_password};endpoint={_endpoint};";

            using var conn = new FireboltConnection(connString);
            conn.Open();

            var cursor = conn.CreateCursor();

            var value = cursor.Execute(commandText);
            Assert.IsNotEmpty(value.Data);
        }

	[Ignore("sleepEachRow function is currently not supported on staging")]
        [TestCase("select sleepEachRow(1) from numbers(5)")]
        public void ExecuteSetTest(string commandText)
        {
            var connString = $"database={_database};username={_username};password={_password};endpoint={_endpoint};account={_account}";

            using var conn = new FireboltConnection(connString);
            conn.Open();

            var cursor = conn.CreateCursor();
            cursor.Execute("SET use_standard_sql=0");

            var value = cursor.Execute(commandText);
            Assert.IsNotEmpty(value.Data);
        }

        [TestCase("select * from information_schema.tables")]
        public void ExecuteSetEngineTest(string commandText)
        {
            var connString = $"database={_database};username={_username};password={_password};endpoint={_endpoint};account={_account}";

            using var conn = new FireboltConnection(connString);
            conn.Open();
            conn.SetEngine(_engine);

            var value = conn.CreateCursor().Execute(commandText);
            Assert.IsNotEmpty(value.Data);
        }
    }
}
