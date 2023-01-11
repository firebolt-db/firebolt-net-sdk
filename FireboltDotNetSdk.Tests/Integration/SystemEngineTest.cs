using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;

namespace FireboltDotNetSdk.Tests
{

    [TestFixture]
    internal class SystemEngineTest : IntegrationTest
    {


        private FireboltConnection? Connection;
        private static string EngineName = "system_engine_dotnet_test";
        private static string DatabaseName = "system_engine_dotnet_test";

        [OneTimeSetUp]
        public void Init()
        {
            var connString = $"database={Database};username={Username};password={Password};endpoint={Endpoint};";
            Connection = new FireboltConnection(connString);
            Connection.Open();
            Connection.SetEngine("system");

            var cursor = Connection.CreateCursor();

            CreateEngine(cursor, EngineName, "SPEC = B1");
            CreateDatabase(cursor, DatabaseName, EngineName);
            Thread.Sleep(10000);
        }

        [OneTimeTearDown]
        public void Cleanup()
        {
            if (Connection != null)
            {
                var cursor = Connection.CreateCursor();
                try
                {
                    cursor.Execute($"DROP ENGINE IF EXISTS {EngineName}");
                }
                catch (FireboltException) { };
                try
                {
                    cursor.Execute($"DROP DATABASE IF EXISTS {DatabaseName}");
                }
                catch (FireboltException) { };
            }
        }

        private void CreateDatabase(FireboltCommand cursor, string dbName, string? attachedEngine = null)
        {
            try
            {
                cursor.Execute($"DROP DATABASE {dbName}");
            }
            catch (FireboltException) { };

            string sql = $"CREATE DATABASE IF NOT EXISTS {dbName}";
            if (attachedEngine != null)
            {
                sql += $" WITH ATTACHED_ENGINES = ('{attachedEngine}')";
            }
            cursor.Execute(sql);
        }
        private void CreateEngine(FireboltCommand cursor, string engineName, string? spec = null)
        {
            try
            {
                cursor.Execute($"DROP ENGINE {engineName}");
            }
            catch (FireboltException ex)
            {
            };

            var create_engine_sql = $"CREATE ENGINE IF NOT EXISTS {engineName}";
            if (spec != null)
            {
                create_engine_sql += $" WITH {spec}";
            }
            cursor.Execute(create_engine_sql);
        }

        [TestCase("CREATE DIMENSION TABLE dummy(id INT)")]
        [TestCase("SHOW TABLES")]
        [TestCase("SHOW INDEXES")]
        public void ErrorsTest(string query)
        {
            var cursor = Connection.CreateCursor();

            Assert.Throws<FireboltException>(
        () => { cursor.Execute(query); }
            );
        }

        [Test]
        public void ShowDatabasesTest()
        {
            var cursor = Connection.CreateCursor();

            var res = cursor.Execute("SHOW DATABASES");

            Assert.That(res.Data.Select(item => item[0]), Has.Exactly(1).EqualTo(DatabaseName));

        }

        public void CheckEngineExistsWithDB(FireboltCommand cursor, string engineName, string dbName)
        {
            var res = cursor.Execute("SHOW ENGINES");

            Assert.That(
        res.Data.Select(item => item[0]), Has.Exactly(1).EqualTo(engineName),
        "Engine {engineName} is missing in SHOW ENGINES"
        );
            Assert.That(
        res.Data.Select(item => new object[] { item[0], item[5] }), Has.Exactly(1).EqualTo(new object[] { engineName, dbName }),
        $"Engine {engineName} doesn't have {dbName} database attached in SHOW ENGINES"
        );
        }

        [Test]
        public void AttachDetachEngineTest()
        {
            var cursor = Connection.CreateCursor();

            CheckEngineExistsWithDB(cursor, EngineName, DatabaseName);

            cursor.Execute($"DETACH ENGINE {EngineName} FROM {DatabaseName}");

            CheckEngineExistsWithDB(cursor, EngineName, "-");

            cursor.Execute($"ATTACH ENGINE {EngineName} TO {DatabaseName}");

            CheckEngineExistsWithDB(cursor, EngineName, DatabaseName);

        }
        private void VerifyEngineSpec(FireboltCommand cursor, string engineName, string spec)
        {
            var res = cursor.Execute($"SHOW ENGINES");

            Assert.That(
        res.Data.Select(item => item[0]), Has.Exactly(1).EqualTo(engineName),
        $"Engine {engineName} is missing in SHOW ENGINES"
        );
            Assert.That(
            res.Data.Select(item => new object[] { item[0], item[2] }), Has.Exactly(1).EqualTo(new object[] { EngineName, spec }),
            $"Engine {engineName} should have {spec} spec in SHOW ENGINES"
            );
        }

        [Test]
        public void AlterEngineTest()
        {
            var cursor = Connection.CreateCursor();

            VerifyEngineSpec(cursor, EngineName, "B1");

            cursor.Execute($"ALTER ENGINE {EngineName} SET SPEC = B2");

            VerifyEngineSpec(cursor, EngineName, "B2");

        }

        private void VerifyEngineStatus(FireboltCommand cursor, string engineName, string status)
        {
            var res = cursor.Execute("SHOW ENGINES");

            Assert.That(
            res.Data.Select(item => item[0]), Has.Exactly(1).EqualTo(engineName),
            "Engine {engineName} is missing in SHOW ENGINES"
            );
            Assert.That(
            res.Data.Select(item => new object[] { item[0], item[4] }), Has.Exactly(1).EqualTo(new object[] { engineName, status }),
            "Engine {engineName} should have {status} status"
            );
        }

        [Test]
        public void StartStopEngineTest()
        {
            var cursor = Connection.CreateCursor();

            VerifyEngineStatus(cursor, EngineName, "Stopped");

            cursor.Execute($"START ENGINE {EngineName}");

            VerifyEngineStatus(cursor, EngineName, "Running");

            cursor.Execute($"STOP ENGINE {EngineName}");

            VerifyEngineStatus(cursor, EngineName, "Stopped");

        }

    }
}
