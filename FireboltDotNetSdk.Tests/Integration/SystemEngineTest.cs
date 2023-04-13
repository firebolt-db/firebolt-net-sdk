using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;

namespace FireboltDotNetSdk.Tests
{

    [TestFixture]
    internal class SystemEngineTest : IntegrationTest
    {
        private FireboltConnection Connection = null!;
        private static int suffix = new Random().Next(9999999);
        private static string newEngineName = "system_engine_dotnet_test_" + suffix;
        private static string newDatabaseName = "system_engine_dotnet_test_" + suffix;

        [OneTimeSetUp]
        public void Init()
        {
            var connString = $"database={Database};username={ClientId};password={ClientSecret};endpoint={Endpoint};engine=system";
            Connection = new FireboltConnection(connString);
            Connection.Open();
            var cursor = Connection.CreateCursor();
            CreateEngine(cursor, newEngineName, "SPEC = B1");
            CreateDatabase(cursor, newDatabaseName, newEngineName);
        }

        [OneTimeTearDown]
        public void Cleanup()
        {
            if (Connection != null)
            {
                var cursor = Connection.CreateCursor();
                try
                {
                    //We first attach the engine to an existing database as it can't be dropped if not attached
                    cursor.Execute($"ATTACH ENGINE {newEngineName} TO {Database}");
                    cursor.Execute($"DROP ENGINE IF EXISTS {newEngineName}");
                }
                catch (System.Exception) { }
                try
                {
                    cursor.Execute($"DROP DATABASE IF EXISTS {newDatabaseName}");
                }
                catch (FireboltException) { };
            }
        }

        private void CreateDatabase(FireboltCommand cursor, string dbName, string? attachedEngine = null)
        {
            string sql = $"CREATE DATABASE IF NOT EXISTS {dbName}";
            if (attachedEngine != null)
            {
                sql += $" WITH ATTACHED_ENGINES = ('{attachedEngine}')";
            }
            cursor.Execute(sql);
        }
        private void CreateEngine(FireboltCommand cursor, string engineName, string? spec = null)
        {
            var create_engine_sql = $"CREATE ENGINE IF NOT EXISTS {engineName}";
            if (spec != null)
            {
                create_engine_sql += $" WITH {spec}";
            }
            cursor.Execute(create_engine_sql);
        }

        [TestCase("CREATE DIMENSION TABLE dummy(id INT)")]
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
            Assert.NotNull(res);
            Assert.That(res!.Data.Select(item => item[0]), Has.Exactly(1).EqualTo(newDatabaseName));

        }

        private void CheckEngineExistsWithDB(FireboltCommand cursor, string engineName, string dbName)
        {
            var res = cursor.Execute("SHOW ENGINES");

            Assert.NotNull(res);
            Assert.That(
        res!.Data.Select(item => item[0]), Has.Exactly(1).EqualTo(engineName),
        "Engine {engineName} is missing in SHOW ENGINES"
        );
            Assert.That(
                res.Data.Select(
                    item => new object[] { item[0] ?? "", item[5] ?? "" }
                ),
                Has.Exactly(1).EqualTo(new object[] { engineName, dbName }),
                $"Engine {engineName} doesn't have {dbName} database attached in SHOW ENGINES"
            );
        }

        [Test]
        public void AttachDetachEngineTest()
        {
            var cursor = Connection.CreateCursor();

            CheckEngineExistsWithDB(cursor, newEngineName, newDatabaseName);

            cursor.Execute($"DETACH ENGINE {newEngineName} FROM {newDatabaseName}");

            CheckEngineExistsWithDB(cursor, newEngineName, "-");

            cursor.Execute($"ATTACH ENGINE {newEngineName} TO {newDatabaseName}");

            CheckEngineExistsWithDB(cursor, newEngineName, newDatabaseName);

        }
        private void VerifyEngineSpec(FireboltCommand cursor, string engineName, string spec)
        {
            var res = cursor.Execute($"SHOW ENGINES");

            Assert.NotNull(res);
            Assert.That(
                res!.Data.Select(
                    item => item[0]
                ),
                Has.Exactly(1).EqualTo(engineName),
                $"Engine {engineName} is missing in SHOW ENGINES"
            );
            Assert.That(
                res.Data.Select(
                    item => new object[] { item[0] ?? "", item[2] ?? "" }
                ),
                Has.Exactly(1).EqualTo(new object[] { newEngineName, spec }),
                $"Engine {engineName} should have {spec} spec in SHOW ENGINES"
            );
        }

        [Test]
        public void AlterEngineTest()
        {
            var cursor = Connection.CreateCursor();

            VerifyEngineSpec(cursor, newEngineName, "B1");

            cursor.Execute($"ALTER ENGINE {newEngineName} SET SPEC = B2");

            VerifyEngineSpec(cursor, newEngineName, "B2");

        }

        private void VerifyEngineStatus(FireboltCommand cursor, string engineName, string status)
        {
            var res = cursor.Execute("SHOW ENGINES");

            Assert.NotNull(res);
            Assert.That(
            res!.Data.Select(item => item[0]), Has.Exactly(1).EqualTo(engineName),
            "Engine {engineName} is missing in SHOW ENGINES"
            );
            Assert.That(
                res.Data.Select(
                    item => new object[] { item[0] ?? "", item[4] ?? "" }
                ),
                Has.Exactly(1).EqualTo(new object[] { engineName, status }),
                "Engine {engineName} should have {status} status"
            );
        }

        [Test]
        public void StartStopEngineAndDropDbTest()
        {
            var cursor = Connection.CreateCursor();

            VerifyEngineStatus(cursor, newEngineName, "Stopped");

            cursor.Execute($"START ENGINE {newEngineName}");

            VerifyEngineStatus(cursor, newEngineName, "Running");
            Thread.Sleep(30000);
            cursor.Execute($"STOP ENGINE {newEngineName}");

            VerifyEngineStatus(cursor, newEngineName, "Stopped");
            cursor.Execute($"DROP DATABASE IF EXISTS {newDatabaseName}");

        }

    }
}
