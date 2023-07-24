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
            Connection.Open();
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

            FireboltException? exception = Assert.Throws<FireboltException>(
                        () => Connection.Open());
            Assert.NotNull(exception);
            Assert.That(exception!.Message, Does.Contain("Account with name non-existing-account-123 was not found"));
        }
    }
}
