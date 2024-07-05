using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using System.Data.Common;
using System.Data;

namespace FireboltDotNetSdk.Tests
{

    [TestFixture]
    internal class ConnectionTest : IntegrationTest
    {
        [TestCase(true, false, true, Description = "Connect without engine but with database", Category = "v1")]
        [TestCase(false, true, true, Description = "Connect with engine but without database; used default database", Category = "v2")]
        [TestCase(false, false, false, Description = "Connect without engine and without database", Category = "v2,engine-v2")]
        [TestCase(false, true, false, Description = "Connect with engine but without database", Category = "engine-v2")]
        [TestCase(true, true, true, Description = "Connect with both engine and database", Category = "v1,v2,engine-v2")]
        public void SuccessfulConnectTest(bool useDatabase, bool useEngine, bool expectsDatabase)
        {
            var connString = GetConnectionString(useDatabase, useEngine);
            string expectedDatabase = expectsDatabase ? Database : string.Empty;
            DbConnection connection = new FireboltConnection(connString);
            Assert.That(connection.ConnectionString, Is.EqualTo(connString));

            connection.Open();

            string version = connection.ServerVersion;
            Assert.NotNull(version);
            Assert.IsNotEmpty(version);

            Assert.That(connection.DataSource ?? string.Empty, Is.EqualTo(expectedDatabase));
            DbCommand command = connection.CreateCommand();
            command.CommandText = "SELECT 1";
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

        [TestCase(false, false, null, "Cannot get url of default engine url from  database", Category = "v1")]
        [TestCase(false, true, "SELECT 1", "The operation is unauthorized", Category = "v1")]
        public void UnsuccessfulConnectTest(bool useDatabase, bool useEngine, string query, string errorMessage)
        {
            var connString = GetConnectionString(useDatabase, useEngine);
            DbConnection connection = new FireboltConnection(connString);
            Assert.That(connection.ConnectionString, Is.EqualTo(connString));

            FireboltException? exception = (FireboltException?)Assert.Throws(Is.InstanceOf<FireboltException>(),() =>
            {
                connection.Open();
                if (query != null)
                {
                    DbCommand command = connection.CreateCommand();
                    command.CommandText = query;
                    command.ExecuteReader();
                }
            });
            Assert.That(exception!.Message, Does.Contain(errorMessage));
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

        [TestCase("Account with name non-existing-account-123 was not found", Category = "v1")]
        [TestCase("Account 'non-existing-account-123' does not exist in this organization", Category = "v2,engine-v2")]
        public void InvalidAccountConnectTest(string errorMessage)
        {
            var connString = ConnectionString(new Tuple<string, string?>(nameof(Account), "non-existing-account-123"));
            DbConnection connection = new FireboltConnection(connString);
            FireboltException? exception = (FireboltException?)Assert.Throws(Is.InstanceOf<FireboltException>(),() => connection.Open());
            Assert.NotNull(exception);
            Assert.That(exception!.Message, Does.Contain(errorMessage));
        }

        [Test]
        [Category("general")]
        public void ChangeDatabaseToNotExistingWhenConnectionIsOpen()
        {
            var connString = ConnectionString();
            DbConnection connection = new FireboltConnection(connString);
            Assert.That(connection.ConnectionString, Is.EqualTo(connString));
            connection.Open();
            Assert.Throws(Is.InstanceOf<FireboltException>(),() => connection.ChangeDatabase("DOES_NOT_EXIST"));
        }

        [Test]
        [Category("general")]
        public void ChangeDatabaseToNotExistingWhenConnectionIsNotOpen()
        {
            var connString = ConnectionString();
            DbConnection connection = new FireboltConnection(connString);
            Assert.That(connection.ConnectionString, Is.EqualTo(connString));
            connection.ChangeDatabase("DOES_NOT_EXIST"); // does not fail because connection is not open
            Assert.Throws(Is.InstanceOf<FireboltException>(),() => connection.Open());
        }

        [Test]
        [Category("general")]
        public void ChangeDatabaseToExistingWhenConnectionIsOpen()
        {
            var connString = ConnectionString(new Tuple<string, string?>(nameof(Database), "DOES_NOT_EXIST"));
            DbConnection connection = new FireboltConnection(connString);
            Assert.That(connection.ConnectionString, Is.EqualTo(connString));
            Assert.Throws(Is.InstanceOf<FireboltException>(),() => connection.Open());
            failingSelect(connection);
            connection.ChangeDatabase(Database);
            connection.Open();
            assertSelect(connection.CreateCommand());
            connection.Close();
        }

        [Test]
        [Category("general")]
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
        [Category("general")]
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
        [Category("general")]
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
        [Category("general")]
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
        [Category("engine-v2")]
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
        [Category("engine-v2")]
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
        [Category("general")]
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
            Assert.Throws(Is.InstanceOf<FireboltException>(),() =>
            {
                DbCommand command = connection.CreateCommand();
                command.CommandText = "SELECT 1";
                command.ExecuteReader();
            });
        }

        [Test]
        public void Factory()
        {
            DbProviderFactory factory = DbProviderFactories.GetFactory(new FireboltConnection(ConnectionString()))!;
            DbConnection connection = factory!.CreateConnection()!;
            connection.ConnectionString = ConnectionString();
            connection.Open();
            DbCommand command = factory.CreateCommand()!;
            command.Connection = (FireboltConnection)connection;
            assertSelect(command);
            connection.Close();
        }
    }
}
