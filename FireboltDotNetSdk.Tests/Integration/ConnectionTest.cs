using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using System.Data.Common;

namespace FireboltDotNetSdk.Tests
{

    [TestFixture]
    internal class ConnectionTest : IntegrationTest
    {
        [TestCase(false, false, Description = "Connect without database and engine")]
        [TestCase(true, false, Description = "Connect without engine but with database")]
        [TestCase(false, true, Description = "Connect with engine but without database")]
        [TestCase(true, true, Description = "Connect with both engine and database")]
        public void ConnectTest(bool useDatabase, bool useEngine)
        {
            var connString = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env}";
            string expectedDatabase = string.Empty;
            if (useDatabase)
            {
                connString += $";database={Database}";
                expectedDatabase = Database;
            }
            if (useEngine)
            {
                connString += $";engine={EngineName}";
                expectedDatabase = Database; // if engine is specified the we use default database
            }
            DbConnection connection = new FireboltConnection(connString);
            Assert.That(connection.ConnectionString, Is.EqualTo(connString));
            connection.Open();

            string version = connection.ServerVersion;
            Assert.NotNull(version);
            Assert.IsNotEmpty(version);

            Assert.That(connection.DataSource ?? string.Empty, Is.EqualTo(expectedDatabase));
            DbCommand command = connection.CreateCommand();
            command.CommandText = "SELECT TOP 1 * FROM information_schema.tables";

            if (useEngine || useDatabase)
            {
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
            else
            {
                FireboltException? exception = Assert.Throws<FireboltException>(() => command.ExecuteReader());
            }
        }

        [Test]
        public void InvalidAccountConnectTest()
        {
            var connString = $"clientid={ClientId};clientsecret={ClientSecret};account=non-existing-account-123;env={Env}";
            DbConnection connection = new FireboltConnection(connString);
            FireboltException? exception = Assert.Throws<FireboltException>(() => connection.Open());
            Assert.NotNull(exception);
            Assert.That(exception!.Message, Does.Contain("Account with name non-existing-account-123 was not found"));
        }

        [Test]
        public void ChangeDatabaseToNotExistingWhenConnectionIsOpen()
        {
            var connString = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env};database={Database};engine={EngineName}";
            DbConnection connection = new FireboltConnection(connString);
            Assert.That(connection.ConnectionString, Is.EqualTo(connString));
            connection.Open();
            FireboltException? exception = Assert.Throws<FireboltException>(() => connection.ChangeDatabase("DOES_NOT_EXIST"));
        }

        [Test]
        public void ChangeDatabaseToNotExistingWhenConnectionIsNotOpen()
        {
            var connString = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env};database={Database};engine={EngineName}";
            DbConnection connection = new FireboltConnection(connString);
            Assert.That(connection.ConnectionString, Is.EqualTo(connString));
            connection.ChangeDatabase("DOES_NOT_EXIST"); // does not fail because connection is not open
            FireboltException? exception = Assert.Throws<FireboltException>(() => connection.Open());
        }

        [Test]
        public void ChangeDatabaseToExistingWhenConnectionIsOpen()
        {
            var connString = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env};database=DOES_NOT_EXIST;engine={EngineName}";
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
            var connString = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env};database={Database};engine={EngineName}";
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
            var connString = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env};database={Database};engine={EngineName}";
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
            var connString = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env};database={Database};engine={EngineName}";
            DbConnection connection = new FireboltConnection(connString);
            connection.ConnectionString = connString;
            connection.Open();
            assertSelect(connection.CreateCommand());
            connection.Close();
        }

        [Test]
        public void SetConnectionStringSameValueOpen()
        {
            var connString = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env};database={Database};engine={EngineName}";
            DbConnection connection = new FireboltConnection(connString);
            connection.Open();
            assertSelect(connection.CreateCommand());
            connection.ConnectionString = connString;
            assertSelect(connection.CreateCommand());
            connection.Close();
        }

        [Test]
        public void SetEngineUsingConnectionStringFirstWrongThenGood()
        {
            var connString1 = $"clientid=ClientId;clientsecret=ClientSecret;account=Account;env=Env;database=Database;engine=EngineName";
            var connString2 = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env};database={Database};engine={EngineName}";
            SetConnectionStringFirstWrongThenGood<HttpRequestException>(connString1, connString2);
        }

        [Test]
        public void SetEngineUsingConnectionStringFirstGoodThenWrong()
        {
            var connString1 = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env};database={Database};engine={EngineName}";
            var connString2 = $"clientid=ClientId;clientsecret=ClientSecret;account=Account;env=Env;database=Database;engine=EngineName";
            SetConnectionStringFirstGoodThenWrong<HttpRequestException>(connString1, connString2);
        }

        [Test]
        public void SetAccountUsingConnectionStringFirstGoodThenWrong()
        {
            var connString1 = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env};database={Database};engine={EngineName}";
            var connString2 = $"clientid={ClientId};clientsecret={ClientSecret};account=WRONG;env={Env};database={Database};engine={EngineName}";
            SetConnectionStringFirstGoodThenWrong<FireboltException>(connString1, connString2);
        }

        [Test]
        public void SetDatabaseUsingConnectionStringFirstGoodThenWrong()
        {
            var connString1 = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env};database={Database};engine={EngineName}";
            var connString2 = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env};database=WRONG;engine={EngineName}";
            SetConnectionStringFirstGoodThenWrong<FireboltException>(connString1, connString2);
        }

        private void SetConnectionStringFirstWrongThenGood<E>(string connString1, string connString2) where E : System.Exception
        {
            DbConnection connection = new FireboltConnection(connString1);
            Assert.Throws<HttpRequestException>(() => connection.Open());
            connection.ConnectionString = connString2;
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
