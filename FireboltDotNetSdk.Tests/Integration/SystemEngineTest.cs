using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Text.RegularExpressions;

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
            string? systemEngineName = Endpoint == null ? null : FireboltConnection.SYSTEM_ENGINE;
            string connectionString = ConnectionString(new Tuple<string, string?>[] { Tuple.Create<string, string?>(nameof(Engine), systemEngineName) });
            Connection = new FireboltConnection(connectionString);
            Connection.Open();
            string? engineSpec = Connection.InfraVersion == 1 ? "SPEC = 'B1'" : null;
            string? attachedEngine = null;
            CreateEngine(newEngineName, engineSpec);
            CreateDatabase(newDatabaseName, attachedEngine);
        }

        [OneTimeTearDown]
        public void Cleanup()
        {
            if (Connection == null)
            {
                return;
            }
            if (Connection.InfraVersion == 1)
            {
                //We first attach the engine to an existing database as it can't be dropped if not attached
                executeSafely($"ATTACH ENGINE {newEngineName} TO {Database}");
            }
            executeSafely(
                $"STOP ENGINE {newEngineName}",
                $"DROP ENGINE {newEngineName}",
                $"DROP DATABASE IF EXISTS {newDatabaseName}"
            );
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

        [TestCase("select 1", Category = "general")]
        [TestCase("select count(*) from information_schema.tables where table_name = 'tables'", Category = "engine-v2")]
        public void SuccessfulQueryTest(string query)
        {
            Assert.That(CreateCommand(query).ExecuteScalar(), Is.EqualTo(1));
        }

        [Test]
        [Category("v1")]
        [Description("It is forbidden to create table using system engine")]
        public void CreateTableUsingSystemEngineTest()
        {
            try
            {
                var command = CreateCommand("CREATE TABLE IF NOT EXISTS dummy(id INT)");
                string errorMessage = Assert.Throws<FireboltException>(() => { command.ExecuteNonQuery(); })?.Response ?? "";
                Assert.That(errorMessage.Trim(), Is.EqualTo($"Database '{Database}' does not exist or not authorized."));
            }
            finally
            {
                try
                {
                    CreateCommand("DROP TABLE dummy").ExecuteNonQuery();
                }
                catch (FireboltException) { };
            }
        }

        [Test]
        [Category("v2")]
        [Category("engine-v2")]
        [Description("It is forbidden to select from a table using system engine")]
        public void SelectUsingSystemEngineTest()
        {
            try
            {
                var command = CreateCommand("CREATE TABLE IF NOT EXISTS dummy(id INT); SELECT * FROM dummy");
                string errorMessage = Assert.Throws<FireboltException>(() => { command.ExecuteNonQuery(); })?.Response ?? "";
                Assert.True(new Regex("Run this (query|statement) on a user engine.").Match(errorMessage).Success);
            }
            finally
            {
                try
                {
                    CreateCommand("DROP TABLE dummy").ExecuteNonQuery();
                }
                catch (FireboltException) { };
            }
        }

        [Test]
        [Category("engine-v2")]
        public void ShowDatabasesTest()
        {
            DbCommand command = Connection.CreateCommand();
            command.CommandText = "SHOW DATABASES";
            DbDataReader reader = command.ExecuteReader();
            Assert.NotNull(reader);
            Assert.That(readData(reader), Has.Exactly(1).EqualTo(newDatabaseName));
        }

        [TestCase("SHOW DATABASES", "Should not be called on firebolt V1")]
        [TestCase("select count(*) from information_schema.tables where table_name = 'tables'", "Queries against table information_schema.tables require a specific database.")]
        [Category("v1")]
        public void ForbiddenQuery(string query, string expectedError)
        {
            DbCommand command = Connection.CreateCommand();
            command.CommandText = "SHOW DATABASES";
            Assert.That(Assert.Throws<FireboltException>(() => command.ExecuteReader()).Response!.Trim(), Is.EqualTo("Should not be called on firebolt V1"));
        }

        private void CheckEngineExistsWithDB(DbCommand command, string engineName, string dbName)
        {
            command.CommandText = "SHOW ENGINES";
            DbDataReader reader = command.ExecuteReader();
            Assert.NotNull(reader);
            Assert.That(readData(reader), Has.Exactly(1).EqualTo(newDatabaseName));

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
            Assert.That(readData(reader), Has.Exactly(1).EqualTo(engineName));

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
        [Category("v1")]
        [Category("v2")]
        public void AlterEngineTest()
        {
            var command = Connection.CreateCommand();

            VerifyEngineSpec(command, newEngineName, "B1");

            CreateCommand($"ALTER ENGINE {newEngineName} SET SPEC = 'B2'").ExecuteNonQuery();

            VerifyEngineSpec(command, newEngineName, "B2");

        }

        private void VerifyEngineStatus(DbCommand command, string engineName, string status)
        {
            command.CommandText = "select engine_name, status from information_schema.engines";
            DbDataReader reader = command.ExecuteReader();
            Assert.NotNull(reader);
            Assert.That(readData(reader), Has.Exactly(1).EqualTo(engineName), "Engine {engineName} is missing in SHOW ENGINES");

            reader = command.ExecuteReader();
            Assert.That(
                readItems(reader, 0, 1),
                Has.Exactly(1).EqualTo(new object[] { engineName, status }),
                $"Engine {engineName} should have {status} status"
            );
        }

        [TestCase("")]
        [TestCase("DATABASE")]
        [Category("engine-v2")]
        [Category("slow")]
        public void UseDatabaseTest(string entityType)
        {
            string databaseName = Database + "_other_" + suffix;
            string tableName = databaseName + "_table";
            string table1 = tableName + "_1";
            string table2 = tableName + "_2";

            try
            {
                CreateCommand($"use {entityType} {Database}").ExecuteNonQuery(); // use current DB; shouldn't have any effect
                Assert.IsNull(GetTableDbName(table1)); // the table does not exist yet
                CreateCommand($"CREATE TABLE {table1} ( id LONG)").ExecuteNonQuery(); // create table1 in current DB
                Assert.That(GetTableDbName(table1), Is.EqualTo(Database)); // now table t1 exists
                Assert.Throws<FireboltException>(() => CreateCommand($"use {entityType} {databaseName}").ExecuteNonQuery()); // DB does not exist
                CreateCommand($"CREATE DATABASE IF NOT EXISTS {databaseName}").ExecuteNonQuery(); // create DB
                CreateCommand($"use {entityType} {databaseName}").ExecuteNonQuery(); // Now this should succeed            
                CreateCommand($"CREATE TABLE {table2} ( id LONG)").ExecuteNonQuery(); // create table2 in other DB
                Assert.IsNull(GetTableDbName(table1)); // table1 does not exist here
                Assert.That(GetTableDbName(table2), Is.EqualTo(databaseName)); // but table2 does exist
            }
            finally
            {
                executeSafely(
                    $"USE {entityType} {databaseName}",
                    $"DROP TABLE {table2}",
                    $"DROP DATABASE {databaseName}",
                    $"USE {entityType} {Database}",
                    $"DROP TABLE {table1}"
                );
            }
        }


        [Test]
        [Category("engine-v2")]
        [Category("slow")]
        public void UseEngineTest()
        {
            string databaseName = Database + "_other_" + suffix;
            string engineName = databaseName + "_engine";
            string tableName = databaseName + "_table";
            string table1 = tableName + "_1";
            string table2 = tableName + "_2";

            try
            {
                CreateCommand("USE ENGINE SYSTEM").ExecuteNonQuery();
                Assert.Throws<FireboltException>(() => CreateCommand($"use ENGINE {engineName}").ExecuteNonQuery());
                CreateCommand($"CREATE ENGINE {engineName}").ExecuteNonQuery();
                CreateCommand($"USE ENGINE {engineName}").ExecuteNonQuery();
                CreateCommand($"CREATE DATABASE IF NOT EXISTS {databaseName}").ExecuteNonQuery();
                CreateCommand($"USE DATABASE {databaseName}").ExecuteNonQuery();
                CreateCommand($"CREATE TABLE {table1} ( id LONG)").ExecuteNonQuery();
                CreateCommand($"INSERT INTO {table1} (id) VALUES (1)").ExecuteNonQuery();// should succeed using user engine
                // switch back to the system engine
                CreateCommand("USE ENGINE SYSTEM").ExecuteNonQuery();
                Assert.Throws<FireboltException>(() => CreateCommand($"INSERT INTO {table1} (id) VALUES (1)").ExecuteNonQuery());
            }
            finally
            {
                executeSafely(
                    $"USE DATABASE {databaseName}",
                    $"DROP TABLE {table1}",
                    $"DROP DATABASE {databaseName}",
                    $"STOP ENGINE {engineName}",
                    $"DROP ENGINE {engineName}"
                );
            }
        }

        [Test]
        [Category("engine-v2")]
        [Category("slow")]
        public void UseEngineMixedCase()
        {
            string engineName = "DotNetMixedCaseTest" + suffix;
            try
            {
                CreateCommand("USE ENGINE SYSTEM").ExecuteNonQuery();
                CreateCommand($"CREATE ENGINE \"{engineName}\"").ExecuteNonQuery();
                CreateCommand($"USE ENGINE \"{engineName}\"").ExecuteNonQuery();
                Assert.Throws<FireboltException>(() => CreateCommand($"USE ENGINE {engineName}").ExecuteNonQuery());
            }
            finally
            {
                executeSafely(
                    $"USE ENGINE SYSTEM",
                    $"STOP ENGINE \"{engineName}\"",
                    $"DROP ENGINE \"{engineName}\""
                );
            }
        }

        [Test]
        [Category("engine-v2")]
        [Category("slow")]
        public void UseEngineMixedCaseToLowerCase()
        {
            string engineName = "DotNetToLowerCaseTest" + suffix;
            try
            {
                CreateCommand("USE ENGINE SYSTEM").ExecuteNonQuery();
                // engine name is lower cased because it is not quoted
                CreateCommand($"CREATE ENGINE {engineName}").ExecuteNonQuery();
                CreateCommand($"USE ENGINE {engineName}").ExecuteNonQuery();
                // engine name remains mixed case and statement fails because engine name was not quoted when we created the engine
                Assert.Throws<FireboltException>(() => CreateCommand($"USE ENGINE \"{engineName}\"").ExecuteNonQuery());
            }
            finally
            {
                executeSafely(
                    $"USE ENGINE SYSTEM",
                    $"STOP ENGINE {engineName}",
                    $"DROP ENGINE {engineName}"
                );
            }
        }

        [Test]
        [Category("engine-v2")]
        [Category("slow")]
        public void ConnectToEngineMixedCase()
        {
            string engineName = "DotNetMixedCaseTest" + suffix;
            string databaseName = engineName + "DB";
            string tableName = databaseName + "_table";

            try
            {
                CreateCommand("USE ENGINE SYSTEM").ExecuteNonQuery();
                CreateCommand($"CREATE ENGINE \"{engineName}\"").ExecuteNonQuery();
                CreateCommand($"CREATE DATABASE \"{databaseName}\"").ExecuteNonQuery();

                // now try to connect to newerly created engine
                string connectionString = ConnectionString(new Tuple<string, string?>[]
                {
                    Tuple.Create<string, string?>(nameof(Engine).ToLower(), engineName),
                    Tuple.Create<string, string?>(nameof(Database).ToLower(), databaseName)
                });
                DbConnection connection = new FireboltConnection(connectionString);
                connection.Open();

                DbCommand command = connection.CreateCommand();
                command.CommandText = "SELECT 1";
                Assert.That(command.ExecuteScalar(), Is.EqualTo(1));

                // Create table, insert row into it and and select data. This ensures we're 100% connected to the engine/db and not system engine
                command.CommandText = $"CREATE TABLE {tableName} ( id LONG)";
                command.ExecuteNonQuery();

                command.CommandText = $"INSERT INTO {tableName} (id) VALUES (123)";
                command.ExecuteNonQuery();

                command.CommandText = $"SELECT id from {tableName} where id=123";
                Assert.That(command.ExecuteScalar(), Is.EqualTo(123));

                connection.Close();
            }
            finally
            {
                executeSafely(
                    $"DROP TABLE \"{tableName}\"",
                    $"STOP ENGINE \"{engineName}\"",
                    $"DROP ENGINE \"{engineName}\"",
                    $"DROP DATABASE \"{databaseName}\""
                );
            }
        }

        [Test]
        [Category("v2")]
        [Category("engine-v2")]
        public void ConnectToAccountWithoutUser()
        {
            string sa_account_name = $"{Database}_sa_no_user_{new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds()}";
            try
            {
                CreateCommand($"CREATE SERVICE ACCOUNT \"{sa_account_name}\" WITH DESCRIPTION = 'Ecosytem test with no user'").ExecuteNonQuery();
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
                badConnection.CleanupCache();

                Assert.That(Assert.Throws<FireboltException>(() => badConnection.Open())?.Message, Does.Match("([Aa]ccount '.+?' does not exist)|(Received an unexpected status code from the server)"));
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

        private List<string> readData(DbDataReader reader)
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

        private void executeSafely(params string[] commands)
        {
            foreach (string cmd in commands)
            {
                try
                {
                    CreateCommand(cmd).ExecuteNonQuery();
                }
                catch (FireboltException) { } // ignore possible exception
            }
        }
    }
}
