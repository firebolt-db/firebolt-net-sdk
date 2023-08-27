using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;


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
            if (useDatabase)
            {
                connString += $";database={Database}";
            }
            if (useEngine)
            {
                connString += $";engine={EngineName}";
            }
            FireboltConnection Connection = new FireboltConnection(connString);
            Assert.That(Connection.ConnectionString, Is.EqualTo(connString));
            Connection.Open();

            string version = Connection.ServerVersion;
            Assert.NotNull(version);
            Assert.IsNotEmpty(version);
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
        public void ChangeDatabaseToNotExistingWhenConnectionIsOpen() {
            var connString = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env};database={Database};engine={EngineName}";
            FireboltConnection Connection = new FireboltConnection(connString);
            Assert.That(Connection.ConnectionString, Is.EqualTo(connString));
            Connection.Open();
            FireboltException? exception = Assert.Throws<FireboltException>(() => Connection.ChangeDatabase("DOES_NOT_EXIST"));
        }

        [Test]
        public void ChangeDatabaseToNotExistingWhenConnectionIsNotOpen() {
            var connString = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env};database={Database};engine={EngineName}";
            FireboltConnection Connection = new FireboltConnection(connString);
            Assert.That(Connection.ConnectionString, Is.EqualTo(connString));
            Connection.ChangeDatabase("DOES_NOT_EXIST"); // does not fail because connection is not open
            FireboltException? exception = Assert.Throws<FireboltException>(() => Connection.Open());
        }

        [Test]
        public void ChangeDatabaseToExistingWhenConnectionIsOpen() {
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
        public void ChangeDatabaseToSameConnectionIsOpen() {
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
        public void ChangeDatabaseToSameConnectionNotOpen() {
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

        private void assertSelectOne(FireboltCommand cursor) {
            var resp = cursor.Execute("SELECT 1");
            Assert.NotNull(resp);
            Assert.That(resp!.Rows, Is.EqualTo(1));
            Assert.That(resp.Data[0][0], Is.EqualTo(1));
        }
    }
}
