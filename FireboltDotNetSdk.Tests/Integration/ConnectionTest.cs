using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using System.Net.Http;


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
            FireboltConnection Connection = new FireboltConnection(connString);
            Assert.That(Connection.ConnectionString, Is.EqualTo(connString));
            Connection.Open();

            string version = Connection.ServerVersion;
            Assert.NotNull(version);
            Assert.IsNotEmpty(version);

            Assert.That(Connection.DataSource ?? string.Empty, Is.EqualTo(expectedDatabase));
            var cursor = Connection.CreateCursor();

            if (useEngine || useDatabase)
            {
                // Test case for use engine
                var resp = cursor.Execute("SELECT TOP 1 * FROM information_schema.tables");
                Assert.NotNull(resp);
                Assert.That(resp!.Rows, Is.GreaterThan(0));
            }
            else
            {
                FireboltException? exception = Assert.Throws<FireboltException>(
                    () => cursor.Execute("SELECT TOP 1 * FROM information_schema.tables"));
            }
        }

        [Test]
        public void InvalidAccountConnectTest()
        {
            var connString = $"clientid={ClientId};clientsecret={ClientSecret};account=non-existing-account-123;env={Env}";
            FireboltConnection Connection = new FireboltConnection(connString);

            FireboltException? exception = Assert.Throws<FireboltException>(() => Connection.Open());
            Assert.NotNull(exception);
            Assert.That(exception!.Message, Does.Contain("Account with name non-existing-account-123 was not found"));
        }

        [Test]
        public void ChangeDatabaseToNotExistingWhenConnectionIsOpen()
        {
            var connString = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env};database={Database};engine={EngineName}";
            FireboltConnection Connection = new FireboltConnection(connString);
            Assert.That(Connection.ConnectionString, Is.EqualTo(connString));
            Connection.Open();
            FireboltException? exception = Assert.Throws<FireboltException>(() => Connection.ChangeDatabase("DOES_NOT_EXIST"));
        }

        [Test]
        public void ChangeDatabaseToNotExistingWhenConnectionIsNotOpen()
        {
            var connString = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env};database={Database};engine={EngineName}";
            FireboltConnection Connection = new FireboltConnection(connString);
            Assert.That(Connection.ConnectionString, Is.EqualTo(connString));
            Connection.ChangeDatabase("DOES_NOT_EXIST"); // does not fail because connection is not open
            FireboltException? exception = Assert.Throws<FireboltException>(() => Connection.Open());
        }

        [Test]
        public void ChangeDatabaseToExistingWhenConnectionIsOpen()
        {
            var connString = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env};database=DOES_NOT_EXIST;engine={EngineName}";
            FireboltConnection Connection = new FireboltConnection(connString);
            Assert.That(Connection.ConnectionString, Is.EqualTo(connString));
            Assert.Throws<FireboltException>(() => Connection.Open());
            Assert.Throws<FireboltException>(() => Connection.CreateCursor().Execute("SELECT 1"));
            Connection.ChangeDatabase(Database);
            Connection.Open();
            assertSelectOne(Connection.CreateCursor());
            Connection.Close();
        }

        [Test]
        public void ChangeDatabaseToSameConnectionIsOpen()
        {
            var connString = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env};database={Database};engine={EngineName}";
            FireboltConnection Connection = new FireboltConnection(connString);
            Assert.That(Connection.ConnectionString, Is.EqualTo(connString));
            Connection.Open();
            assertSelectOne(Connection.CreateCursor());
            Connection.ChangeDatabase(Database);
            assertSelectOne(Connection.CreateCursor());
            Connection.Close();
        }

        [Test]
        public void ChangeDatabaseToSameConnectionNotOpen()
        {
            var connString = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env};database={Database};engine={EngineName}";
            FireboltConnection Connection = new FireboltConnection(connString);
            Assert.That(Connection.ConnectionString, Is.EqualTo(connString));
            Assert.Throws<FireboltException>(() => Connection.CreateCursor().Execute("SELECT 1"));
            Connection.ChangeDatabase(Database);
            Assert.Throws<FireboltException>(() => Connection.CreateCursor().Execute("SELECT 1"));
            Connection.Open();
            assertSelectOne(Connection.CreateCursor());
            Connection.Close();
        }


        [Test]
        public void SetConnectionStringSameValueNotOpen()
        {
            var connString = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env};database={Database};engine={EngineName}";
            FireboltConnection Connection = new FireboltConnection(connString);
            Connection.ConnectionString = connString;
            Connection.Open();
            assertSelectOne(Connection.CreateCursor());
            Connection.Close();
        }

        [Test]
        public void SetConnectionStringSameValueOpen()
        {
            var connString = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env};database={Database};engine={EngineName}";
            FireboltConnection Connection = new FireboltConnection(connString);
            Connection.Open();
            assertSelectOne(Connection.CreateCursor());
            Connection.ConnectionString = connString;
            assertSelectOne(Connection.CreateCursor());
            Connection.Close();
        }

        [Test]
        public void SetConnectionStringFirstWrongThenGood()
        {
            var connString1 = $"clientid=ClientId;clientsecret=ClientSecret;account=Account;env=Env;database=Database;engine=EngineName";
            var connString2 = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env};database={Database};engine={EngineName}";
            SetConnectionStringFirstWrongThenGood<HttpRequestException>(connString1, connString2);
        }

        [Test]
        public void SetConnectionStringFirstGoodThenWrong()
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

        private void assertSelectOne(FireboltCommand cursor)
        {
            var resp = cursor.Execute("SELECT 1");
            Assert.NotNull(resp);
            Assert.That(resp!.Rows, Is.EqualTo(1));
            Assert.That(resp.Data[0][0], Is.EqualTo(1));
        }

        private void SetConnectionStringFirstWrongThenGood<E>(string connString1, string connString2) where E : System.Exception
        {
            FireboltConnection Connection = new FireboltConnection(connString1);
            Assert.Throws<HttpRequestException>(() => Connection.Open());
            Connection.ConnectionString = connString2;
            assertSelectOne(Connection.CreateCursor());
            Connection.Close();
        }

        private void SetConnectionStringFirstGoodThenWrong<E>(string connString1, string connString2) where E : System.Exception
        {
            FireboltConnection Connection = new FireboltConnection(connString1);
            Connection.Open();
            assertSelectOne(Connection.CreateCursor());
            Assert.Throws<E>(() => Connection.ConnectionString = connString2);
        }
    }
}
