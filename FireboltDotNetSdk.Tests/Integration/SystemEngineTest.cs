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
            string? systemEngineName = Endpoint == null ? null : "system";
            string connectionString = ConnectionString(new Tuple<string, string?>[] { Tuple.Create<string, string?>(nameof(Engine), systemEngineName) });
            Connection = new FireboltConnection(connectionString);
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

        [TestCase("select 1")]
        [TestCase("select count(*) from information_schema.tables where table_name = 'tables'")]
        public void SuccessfulQueryTest(string query)
        {
            Assert.That(CreateCommand(query).ExecuteScalar(), Is.EqualTo(1));
        }

        [TestCase("CREATE TABLE dummy(id INT); SELECT * FROM dummy", Description = "It is forbidden to select from a table using system engine", Category = "v2")]
        public void ErrorsTest(string query)
        {
            var command = CreateCommand(query);
            string errorMessage = Assert.Throws<FireboltException>(() => { command.ExecuteNonQuery(); })?.Message ?? "";
            Assert.True(errorMessage.Contains("Run this query on a user engine."));
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
        [Category("v2")]
        public void AttachDetachEngineTest()
        {
            var command = Connection.CreateCommand();

            CheckEngineExistsWithDB(command, newEngineName, newDatabaseName);
            Assert.That(Assert.Throws<FireboltException>(() => ConnectAndRunQuery())?.Message, Is.EqualTo($"Engine {newEngineName} is not running"));

            CreateCommand($"DETACH ENGINE {newEngineName} FROM {newDatabaseName}").ExecuteNonQuery();

            CheckEngineExistsWithDB(command, newEngineName, "-");
            Assert.That(Assert.Throws<FireboltException>(() => ConnectAndRunQuery())?.Message, Is.EqualTo($"Engine {newEngineName} is not attached to {newDatabaseName}"));

            CreateCommand($"ATTACH ENGINE {newEngineName} TO {newDatabaseName}").ExecuteNonQuery();

            CheckEngineExistsWithDB(command, newEngineName, newDatabaseName);
            Assert.That(Assert.Throws<FireboltException>(() => ConnectAndRunQuery())?.Message, Is.EqualTo($"Engine {newEngineName} is not running"));
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

        private IList<object[]> ConnectAndRunQuery(string query = "SELECT 1")
        {
            var connString = ConnectionString(new Tuple<string, string?>(nameof(Engine), newEngineName), new Tuple<string, string?>(nameof(Database), newDatabaseName));
            using (var userConnection = new FireboltConnection(connString))
            {
                userConnection.Open();
                DbCommand command = userConnection.CreateCommand();
                command.CommandText = query;
                DbDataReader reader = command.ExecuteReader();
                IList<object[]> result = new List<object[]>();
                int n = reader.FieldCount;
                while (reader.Read())
                {
                    object[] row = new object[n];
                    result.Add(row);
                    for (int i = 0; i < n; i++)
                    {
                        row[i] = reader.GetValue(i);
                    }
                }
                return result;
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
        [Category("v1")]
        [Category("slow")]
        public void StartStopEngineAndDropDbTestV1()
        {
            AssertStartStopEngineAndDropDbTest(e => e?.Response, "Engine not found");
        }

        [Test]
        [Category("v2")]
        [Category("slow")]
        public void StartStopEngineAndDropDbTestV2()
        {
            AssertStartStopEngineAndDropDbTest(e => e?.Message, $"Engine {newEngineName} is not running");
        }

        private void AssertStartStopEngineAndDropDbTest(Func<FireboltException?, string?> messageGetter, string errorMessage)
        {
            DbCommand command = Connection.CreateCommand();

            VerifyEngineStatus(command, newEngineName, "Stopped");
            Assert.That(messageGetter.Invoke(Assert.Throws<FireboltException>(() => ConnectAndRunQuery())), Is.EqualTo(errorMessage));

            CreateCommand($"START ENGINE {newEngineName}").ExecuteNonQuery();
            VerifyEngineStatus(command, newEngineName, "Running");
            ConnectAndRunQuery();
            IList<object[]> databases = ConnectAndRunQuery("SELECT database_name FROM information_schema.databases");

            CreateCommand($"STOP ENGINE {newEngineName}").ExecuteNonQuery();
            VerifyEngineStatus(command, newEngineName, "Stopped");
            Assert.That(messageGetter.Invoke(Assert.Throws<FireboltException>(() => ConnectAndRunQuery())), Is.EqualTo(errorMessage));

            CreateCommand($"DROP DATABASE IF EXISTS {newDatabaseName}").ExecuteNonQuery();

            // Validate this here, after the test scenario is done. 
            // Otherwise if this check is right after extracting databases list and fails it will prevent database to be dropped. 
            Assert.True(databases.Select(l => l[0]).Contains(Database));
        }

        [Test]
        [Category("v2")]
        [Category("dev")]
        [Category("slow")]
        public void UseTest()
        {
            string databaseName = Database + "_other_" + suffix;
            string engineName = databaseName + "_engine";
            string tableName = databaseName + "_table";
            string table1 = tableName + "_1";
            string table2 = tableName + "_2";

            try
            {
                CreateCommand($"use {Database}").ExecuteNonQuery(); // use current DB; shouldn't have any effect
                Assert.IsNull(GetTableDbName(table1)); // the table does not exist yet
                CreateCommand($"CREATE TABLE {table1} ( id LONG)").ExecuteNonQuery(); // create table1 in current DB
                Assert.That(GetTableDbName(table1), Is.EqualTo(Database)); // now table t1 exists
                Assert.Throws<FireboltException>(() => CreateCommand($"use {databaseName}").ExecuteNonQuery()); // DB does not exist
                CreateCommand($"CREATE DATABASE IF NOT EXISTS {databaseName}").ExecuteNonQuery(); // create DB
                CreateCommand($"use {databaseName}").ExecuteNonQuery(); // Now this should succeed            
                CreateCommand($"CREATE TABLE {table2} ( id LONG)").ExecuteNonQuery(); // create table2 in other DB
                Assert.IsNull(GetTableDbName(table1)); // table1 does not exist here
                Assert.That(GetTableDbName(table2), Is.EqualTo(databaseName)); // but table2 does exist
            }
            finally
            {
                string[] commands2 = new string[] {
                    $"STOP ENGINE {engineName}",
                    $"DROP ENGINE {engineName}",
                    $"DROP DATABASE {databaseName}",
                };
                foreach (string cmd in commands2)
                {
                    try
                    {
                        CreateCommand(cmd).ExecuteNonQuery();
                    }
                    catch (FireboltException) { } // ignore possible exception
                }
            }
        }

        [Test]
        [Category("v2")]
        public void ConnectToAccountWithoutUser()
        {
            string sa_account_name = $"{Database}_sa_no_user_{new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds()}";
            try
            {
                CreateCommand($"CREATE SERVICE ACCOUNT \"{sa_account_name}\" WITH DESCRIPTION = \"Ecosytem test with no user\"").ExecuteNonQuery();
                DbDataReader reader = CreateCommand($"CALL fb_GENERATESERVICEACCOUNTKEY('{sa_account_name}')").ExecuteReader();
                Assert.IsTrue(reader.Read());

                string clientId = reader.GetString(1);
                string clientSecret = reader.GetString(2);
                if (string.IsNullOrEmpty(clientId)) // Currently this is bugged so retrieve id via a query. FIR-28719
                {
                    clientId = (string)CreateCommand($"SELECT service_account_id FROM information_schema.service_accounts WHERE service_account_name='{sa_account_name}'").ExecuteScalar()!;
                }
                string connectionString = ConnectionString(new Tuple<string, string?>[]
                {
                    Tuple.Create<string, string?>(nameof(ClientId), clientId),
                    Tuple.Create<string, string?>(nameof(ClientSecret), clientSecret)
                });
                var badConnection = new FireboltConnection(connectionString);
                Assert.That(Assert.Throws<FireboltException>(() => badConnection.Open())?.Message, Does.Match($"Account '{Account}' does not exist.+RBAC"));
            }
            finally
            {
                CreateCommand($"DROP SERVICE ACCOUNT {sa_account_name}").ExecuteNonQuery();
            }
        }

        private string? GetTableDbName(string table)
        {
            DbCommand command = Connection.CreateCommand();
            command.CommandText = "select table_catalog from information_schema.tables where table_name=@t";
            command.Parameters.Add(CreateParameter(command, "@t", table));
            return (string?)command.ExecuteScalar();
        }

        private DbParameter CreateParameter(DbCommand command, string name, object? value)
        {
            DbParameter parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value;
            return parameter;
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
