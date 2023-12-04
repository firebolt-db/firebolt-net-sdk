using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using System.Data.Common;

namespace FireboltDotNetSdk.Tests
{

    [TestFixture]
    internal class ConnectionTest : IntegrationTest
    {
        [TestCase(true, false, Description = "Connect without engine but with database")]
        [TestCase(false, true, Description = "Connect with engine but without database", Category = "v2")]
        [TestCase(true, true, Description = "Connect with both engine and database")]
        public void SuccessfulConnectTest(bool useDatabase, bool useEngine)
        {
            var connString = GetConnectionString(useDatabase, useEngine);
            // if engine is specified we use default database
            string expectedDatabase = useDatabase || useEngine ? Database : string.Empty;
            DbConnection connection = new FireboltConnection(connString);
            Assert.That(connection.ConnectionString, Is.EqualTo(connString));

            connection.Open();

            string version = connection.ServerVersion;
            Assert.NotNull(version);
            Assert.IsNotEmpty(version);

            Assert.That(connection.DataSource ?? string.Empty, Is.EqualTo(expectedDatabase));
            DbCommand command = connection.CreateCommand();
            command.CommandText = "SELECT TOP 1 * FROM information_schema.tables";
            // Test case for use engine
            DbDataReader reader = command.ExecuteReader();
            Assert.NotNull(reader);
            Assert.True(reader.Read());
            int n = reader.FieldCount;
            for (int i = 0; i < n; i++)
            {
                if (!reader.IsDBNull(i))
                {
                    Assert.NotNull(reader.GetValue(i));
                }
            }
            Assert.False(reader.Read());
            connection.Close();
        }

        [TestCase(false, false, Description = "Connect without database and engine")]
        [TestCase(false, true, Description = "Connect with engine but without database", Category = "v1")]
        public void FailedConnectTest(bool useDatabase, bool useEngine)
        {
            var connString = GetConnectionString(useDatabase, useEngine);
            // if engine is specified we use default database
            string expectedDatabase = useDatabase || useEngine ? Database : string.Empty;
            DbConnection connection = new FireboltConnection(connString);
            Assert.That(connection.ConnectionString, Is.EqualTo(connString));

            FireboltException? exception = Assert.Throws<FireboltException>(() =>
            {
                connection.Open();
                DbCommand command = connection.CreateCommand();
                command.CommandText = "SELECT TOP 1 * FROM information_schema.tables";
                command.ExecuteReader();
            });
        }

        private string GetConnectionString(bool useDatabase, bool useEngine)
        {
            IList<string> paramsToIgnore = new List<string>(); // list of parameters that will be ignored when creating connection string
            if (!useDatabase)
            {
                paramsToIgnore.Add(nameof(Database));
            }
            if (!useEngine)
            {
                paramsToIgnore.Add(nameof(Engine));
            }
            return ConnectionStringWithout(paramsToIgnore.ToArray());
        }

        [Test]
        public void InvalidAccountConnectTest()
        {
            var connString = ConnectionString(new Tuple<string, string?>(nameof(Account), "non-existing-account-123"));
            DbConnection connection = new FireboltConnection(connString);
            FireboltException? exception = Assert.Throws<FireboltException>(() => connection.Open());
            Assert.NotNull(exception);
            Assert.That(exception!.Message, Does.Contain("Account with name non-existing-account-123 was not found"));
        }

        [Test]
        public void ChangeDatabaseToNotExistingWhenConnectionIsOpen()
        {
            var connString = ConnectionString();
            DbConnection connection = new FireboltConnection(connString);
            Assert.That(connection.ConnectionString, Is.EqualTo(connString));
            connection.Open();
            FireboltException? exception = Assert.Throws<FireboltException>(() => connection.ChangeDatabase("DOES_NOT_EXIST"));
        }

        [Test]
        public void ChangeDatabaseToNotExistingWhenConnectionIsNotOpen()
        {
            var connString = ConnectionString();
            DbConnection connection = new FireboltConnection(connString);
            Assert.That(connection.ConnectionString, Is.EqualTo(connString));
            connection.ChangeDatabase("DOES_NOT_EXIST"); // does not fail because connection is not open
            FireboltException? exception = Assert.Throws<FireboltException>(() => connection.Open());
        }

        [Test]
        public void ChangeDatabaseToExistingWhenConnectionIsOpen()
        {
            var connString = ConnectionString(new Tuple<string, string?>(nameof(Database), "DOES_NOT_EXIST"));
            DbConnection connection = new FireboltConnection(connString);
            Assert.That(connection.ConnectionString, Is.EqualTo(connString));
            Assert.Throws<FireboltException>(() => connection.Open());
            failingSelect(connection);
            connection.ChangeDatabase(Database);
            connection.Open();
            assertSelect(connection.CreateCommand());
            connection.Close();
        }

        [Test]
        public void ChangeDatabaseToSameConnectionIsOpen()
        {
            var connString = ConnectionString();
            DbConnection connection = new FireboltConnection(connString);
            Assert.That(connection.ConnectionString, Is.EqualTo(connString));
            connection.Open();
            assertSelect(connection.CreateCommand());
            connection.ChangeDatabase(Database);
            assertSelect(connection.CreateCommand());
            connection.Close();
        }

        [Test]
        public void ChangeDatabaseToSameConnectionNotOpen()
        {
            var connString = ConnectionString();
            DbConnection connection = new FireboltConnection(connString);
            Assert.That(connection.ConnectionString, Is.EqualTo(connString));
            failingSelect(connection);
            connection.ChangeDatabase(Database);
            failingSelect(connection);
            connection.Open();
            assertSelect(connection.CreateCommand());
            connection.Close();
        }


        [Test]
        public void SetConnectionStringSameValueNotOpen()
        {
            var connString = ConnectionString();
            DbConnection connection = new FireboltConnection(connString);
            connection.ConnectionString = connString;
            connection.Open();
            assertSelect(connection.CreateCommand());
            connection.Close();
        }

        [Test]
        public void SetConnectionStringSameValueOpen()
        {
            var connString = ConnectionString();
            DbConnection connection = new FireboltConnection(connString);
            connection.Open();
            assertSelect(connection.CreateCommand());
            connection.ConnectionString = connString;
            assertSelect(connection.CreateCommand());
            connection.Close();
        }

        [Test]
        [Category("v1")]
        public void SetAllFieldsUsingConnectionStringFirstWrongThenGoodV1()
        {
            var connString1 = ConnectionString(
                new Tuple<string, string?>(nameof(UserName), "User@Name"),
                new Tuple<string, string?>(nameof(Password), "Password"),
                new Tuple<string, string?>(nameof(Account), "Account"),
                new Tuple<string, string?>(nameof(Env), "env"),
                new Tuple<string, string?>(nameof(Database), "Database"),
                new Tuple<string, string?>(nameof(Engine), "Engine"),
                new Tuple<string, string?>(nameof(Endpoint), "api.env.firebolt.io")
            );
            var connString2 = ConnectionStringWithout(nameof(ClientId), nameof(ClientSecret));
            SetConnectionStringFirstWrongThenGood<HttpRequestException>(connString1, connString2);
        }

        [Test]
        [Category("v2")]
        public void SetAllFieldsUsingConnectionStringFirstWrongThenGoodV2()
        {
            var connString1 = ConnectionString(
                new Tuple<string, string?>(nameof(ClientId), "ClientId"),
                new Tuple<string, string?>(nameof(ClientSecret), "ClientSecret"),
                new Tuple<string, string?>(nameof(Account), "Account"),
                new Tuple<string, string?>(nameof(Env), "env"),
                new Tuple<string, string?>(nameof(Database), "Database"),
                new Tuple<string, string?>(nameof(Engine), "Engine"),
                new Tuple<string, string?>(nameof(Endpoint), "api.env.firebolt.io")
            );
            var connString2 = ConnectionStringWithout(nameof(UserName), nameof(Password));
            SetConnectionStringFirstWrongThenGood<HttpRequestException>(connString1, connString2);
        }

        [Test]
        [Category("v1")]
        public void SetAllFieldsUsingConnectionStringFirstGoodThenWrongV1()
        {
            SetAllFieldsUsingConnectionStringFirstGoodThenWrong<FireboltException>(nameof(ClientId), nameof(ClientSecret));
        }

        [Test]
        [Category("v2")]
        public void SetAllFieldsUsingConnectionStringFirstGoodThenWrongV2()
        {
            SetAllFieldsUsingConnectionStringFirstGoodThenWrong<HttpRequestException>(nameof(UserName), nameof(Password));
        }
        private void SetAllFieldsUsingConnectionStringFirstGoodThenWrong<E>(params string[] restrictedNames) where E : System.Exception
        {
            var connString1 = ConnectionStringWithout(restrictedNames);
            var connString2 = ConnectionString(
                new Tuple<string, string?>(nameof(ClientId), "ClientId"),
                new Tuple<string, string?>(nameof(ClientSecret), "ClientSecret"),
                new Tuple<string, string?>(nameof(Account), "Account"),
                new Tuple<string, string?>(nameof(Env), "Env"),
                new Tuple<string, string?>(nameof(Database), "Database"),
                new Tuple<string, string?>(nameof(Engine), "Engine")
            );
            SetConnectionStringFirstGoodThenWrong<E>(connString1, connString2);
        }

        [TestCase(nameof(Account))]
        [TestCase(nameof(Database))]
        public void SetFieldUsingConnectionStringFirstGoodThenWrong(string fieldName)
        {
            var connString1 = ConnectionString();
            var connString2 = ConnectionString(new Tuple<string, string?>(fieldName, "WRONG"));
            SetConnectionStringFirstGoodThenWrong<FireboltException>(connString1, connString2);
        }

        private void SetConnectionStringFirstWrongThenGood<E>(string connString1, string connString2) where E : System.Exception
        {
            DbConnection connection = new FireboltConnection(connString1);
            Assert.Throws<E>(() => connection.Open());
            connection.ConnectionString = connString2;
            connection.Open();
            assertSelect(connection.CreateCommand());
            connection.Close();
        }

        private void SetConnectionStringFirstGoodThenWrong<E>(string connString1, string connString2) where E : System.Exception
        {
            DbConnection connection = new FireboltConnection(connString1);
            connection.Open();
            assertSelect(connection.CreateCommand());
            Assert.Throws<E>(() => connection.ConnectionString = connString2);
        }

        private void assertSelect(DbCommand command)
        {
            command.CommandText = "SELECT 1";
            DbDataReader reader = command.ExecuteReader();
            Assert.True(reader.Read());
            Assert.That(reader.GetInt16(0), Is.EqualTo(1));
            Assert.False(reader.Read());
        }

        private void failingSelect(DbConnection connection)
        {
            Assert.Throws<FireboltException>(() =>
            {
                DbCommand command = connection.CreateCommand();
                command.CommandText = "SELECT 1";
                command.ExecuteReader();
            });
        }
    }
}
