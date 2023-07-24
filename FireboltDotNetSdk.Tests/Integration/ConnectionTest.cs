using FireboltDotNetSdk.Client;


namespace FireboltDotNetSdk.Tests
{

    [TestFixture]
    internal class ConnectionTest : IntegrationTest
    {
        [Test]
        public void ConnectNoEngineTest()
        {
            var connString = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env}";
            FireboltConnection Connection = new FireboltConnection(connString);
            Connection.Open();
            var cursor = Connection.CreateCursor();
            cursor.Execute("SELECT TOP 1 * FROM information_schema.tables");
            Assert.NotNull(cursor.Response);
            NewMeta newMeta = ResponseUtilities.getFirstRow(cursor.Response!);
            Assert.That(newMeta.Data[0], Is.EqualTo(1));
        }
        [Test]
        public void ConnectNoEngineWithDatabaseTest()
        {
            var connString = $"database={Database};clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env}";
            FireboltConnection Connection = new FireboltConnection(connString);
            Connection.Open();
            var cursor = Connection.CreateCursor();
            cursor.Execute("SELECT TOP 1 * FROM information_schema.tables");
            Assert.NotNull(cursor.Response);
            NewMeta newMeta = ResponseUtilities.getFirstRow(cursor.Response!);
            Assert.That(newMeta.Data[0], Is.EqualTo(1));
        }
        [Test]
        public void ConnectUserEngineNoDbTest()
        {
            var connString = $"clientid={ClientId};clientsecret={ClientSecret};account={Account};env={Env};engine={EngineName}";
            FireboltConnection Connection = new FireboltConnection(connString);
            Connection.Open();
            var cursor = Connection.CreateCursor();
            cursor.Execute("SELECT TOP 1 * FROM information_schema.tables");
            Assert.NotNull(cursor.Response);
            NewMeta newMeta = ResponseUtilities.getFirstRow(cursor.Response!);
            Assert.That(newMeta.Data[0], Is.EqualTo(1));
        }
    }
}
