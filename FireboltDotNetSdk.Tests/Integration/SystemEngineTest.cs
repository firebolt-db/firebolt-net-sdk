using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using System.Data;
using System.Data.Common;

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
            var connString = $"database={Database};clientid={ClientId};clientsecret={ClientSecret};endpoint={Endpoint};account={Account};env={Env}";
            Connection = new FireboltConnection(connString);
            Connection.Open();
            CreateEngine(newEngineName, "SPEC = 'B1'");
            CreateDatabase(newDatabaseName, newEngineName);
        }

        [OneTimeTearDown]
        public void Cleanup()
        {
            if (Connection != null)
            {
                try
                {
                    //We first attach the engine to an existing database as it can't be dropped if not attached
                    CreateCommand($"ATTACH ENGINE {newEngineName} TO {Database}").ExecuteNonQuery();
                    CreateCommand($"DROP ENGINE IF EXISTS {newEngineName}").ExecuteNonQuery();
                }
                catch (System.Exception) { }
                try
                {
                    CreateCommand($"DROP DATABASE IF EXISTS {newDatabaseName}").ExecuteNonQuery();
                }
                catch (FireboltException) { };
            }
        }

        private void CreateDatabase(string dbName, string? attachedEngine = null)
        {
            string sql = $"CREATE DATABASE IF NOT EXISTS {dbName}";
            if (attachedEngine != null)
            {
                sql += $" WITH ATTACHED_ENGINES = ('{attachedEngine}')";
            }
            CreateCommand(sql).ExecuteNonQuery();
        }
        private void CreateEngine(string engineName, string? spec = null)
        {
            var create_engine_sql = $"CREATE ENGINE IF NOT EXISTS {engineName}";
            if (spec != null)
            {
                create_engine_sql += $" WITH {spec}";
            }
            CreateCommand(create_engine_sql).ExecuteNonQuery();
        }

        [TestCase("CREATE DIMENSION TABLE dummy(id INT)")]
        public void ErrorsTest(string query)
        {
            var command = CreateCommand(query);
            Assert.Throws<FireboltException>(() => { command.ExecuteNonQuery(); });
        }

        [Test]
        public void ShowDatabasesTest()
        {
            DbCommand command = Connection.CreateCommand();
            command.CommandText = "SHOW DATABASES";
            DbDataReader reader = command.ExecuteReader();
            Assert.NotNull(reader);
            Assert.That(readDabaseses(reader), Has.Exactly(1).EqualTo(newDatabaseName));
        }

        private void CheckEngineExistsWithDB(DbCommand command, string engineName, string dbName)
        {
            command.CommandText = "SHOW ENGINES";
            DbDataReader reader = command.ExecuteReader();
            Assert.NotNull(reader);
            Assert.That(readDabaseses(reader), Has.Exactly(1).EqualTo(newDatabaseName));

            reader = command.ExecuteReader();
            Assert.That(
                readItems(reader, 0, 5),
                Has.Exactly(1).EqualTo(new object[] { engineName, dbName }),
                $"Engine {engineName} doesn't have {dbName} database attached in SHOW ENGINES"
            );
        }

        [Test]
        public void AttachDetachEngineTest()
        {
            var command = Connection.CreateCommand();

            CheckEngineExistsWithDB(command, newEngineName, newDatabaseName);
            Assert.That(Assert.Throws<FireboltException>(() => ConnectAndRunQuery()).Message, Is.EqualTo($"Engine {newEngineName} is not running"));

            CreateCommand($"DETACH ENGINE {newEngineName} FROM {newDatabaseName}").ExecuteNonQuery();

            CheckEngineExistsWithDB(command, newEngineName, "-");
            Assert.That(Assert.Throws<FireboltException>(() => ConnectAndRunQuery()).Message, Is.EqualTo($"Engine {newEngineName} is not attached to {newDatabaseName}"));

            CreateCommand($"ATTACH ENGINE {newEngineName} TO {newDatabaseName}").ExecuteNonQuery();

            CheckEngineExistsWithDB(command, newEngineName, newDatabaseName);
            Assert.That(Assert.Throws<FireboltException>(() => ConnectAndRunQuery()).Message, Is.EqualTo($"Engine {newEngineName} is not running"));
        }

        private void VerifyEngineSpec(DbCommand command, string engineName, string spec)
        {
            command.CommandText = "SHOW ENGINES";
            DbDataReader reader = command.ExecuteReader();
            Assert.NotNull(reader);
            Assert.That(readDabaseses(reader), Has.Exactly(1).EqualTo(engineName));

            reader = command.ExecuteReader();
            Assert.That(
                readItems(reader, 0, 2),
                Has.Exactly(1).EqualTo(new object[] { newEngineName, spec }),
                $"Engine {engineName} should have {spec} spec in SHOW ENGINES"
            );
        }

        private void ConnectAndRunQuery()
        {
            var connString = $"database={Database};clientid={ClientId};clientsecret={ClientSecret};endpoint={Endpoint};account={Account};env={Env};engine={newEngineName};database={newDatabaseName}";
            using (var userConnection = new FireboltConnection(connString))
            {
                userConnection.Open();
                DbCommand command = userConnection.CreateCommand();
                command.CommandText = "SELECT 1";
                command.ExecuteReader();
            }
        }

        [Test]
        public void AlterEngineTest()
        {
            var command = Connection.CreateCommand();

            VerifyEngineSpec(command, newEngineName, "B1");

            CreateCommand($"ALTER ENGINE {newEngineName} SET SPEC = 'B2'").ExecuteNonQuery();

            VerifyEngineSpec(command, newEngineName, "B2");

        }

        private void VerifyEngineStatus(DbCommand command, string engineName, string status)
        {
            command.CommandText = "SHOW ENGINES";
            DbDataReader reader = command.ExecuteReader();
            Assert.NotNull(reader);
            Assert.That(readDabaseses(reader), Has.Exactly(1).EqualTo(engineName), "Engine {engineName} is missing in SHOW ENGINES");

            reader = command.ExecuteReader();
            Assert.That(
                readItems(reader, 0, 4),
                Has.Exactly(1).EqualTo(new object[] { engineName, status }),
                $"Engine {engineName} should have {status} status"
            );
        }

        [Test]
        public void StartStopEngineAndDropDbTest()
        {
            DbCommand command = Connection.CreateCommand();

            VerifyEngineStatus(command, newEngineName, "Stopped");
            Assert.That(Assert.Throws<FireboltException>(() => ConnectAndRunQuery()).Message, Is.EqualTo($"Engine {newEngineName} is not running"));

            CreateCommand($"START ENGINE {newEngineName}").ExecuteNonQuery();
            VerifyEngineStatus(command, newEngineName, "Running");
            ConnectAndRunQuery();

            CreateCommand($"STOP ENGINE {newEngineName}").ExecuteNonQuery();
            VerifyEngineStatus(command, newEngineName, "Stopped");
            Assert.That(Assert.Throws<FireboltException>(() => ConnectAndRunQuery()).Message, Is.EqualTo($"Engine {newEngineName} is not running"));

            CreateCommand($"DROP DATABASE IF EXISTS {newDatabaseName}").ExecuteNonQuery();
        }

        private DbCommand CreateCommand(string sql)
        {
            DbCommand command = Connection.CreateCommand();
            command.CommandText = sql;
            return command;
        }

        private List<string> readDabaseses(DbDataReader reader)
        {
            List<string> databases = new List<string>();
            while (reader.Read())
            {
                databases.Add(reader.GetString(0));
            }
            return databases;
        }

        private List<object[]> readItems(DbDataReader reader, params int[] indexes)
        {
            List<object[]> rows = new List<object[]>();
            while (reader.Read())
            {
                object[] row = new object[indexes.Length];
                for (int i = 0; i < indexes.Length; i++)
                {
                    row[i] = reader.IsDBNull(indexes[i]) ? "" : reader.GetString(indexes[i]);
                }
                rows.Add(row);
            }
            return rows;
        }
    }
}
