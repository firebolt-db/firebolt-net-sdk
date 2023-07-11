using System.Data;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using static NUnit.Framework.Assert;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    public class FireboltConnectionTest
    {

        [Test]
        public void ParsingNormalConnectionStringTest()
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=testpwd;account=accountname;endpoint=api.mock.firebolt.io";
            var cs = new FireboltConnection(connectionString);
            Multiple(() =>
            {
                That(cs.Endpoint, Is.EqualTo("api.mock.firebolt.io"));
		That(cs.Env, Is.EqualTo("mock"));
                That(cs.Database, Is.EqualTo("testdb.ib"));
                That(cs.Account, Is.EqualTo("accountname"));
                That(cs.ClientSecret, Is.EqualTo("testpwd"));
                That(cs.ClientId, Is.EqualTo("testuser"));
            });
        }

        [TestCase("database=testdb.ib;clientid=testuser;clientsecret=;account=accountname;endpoint=api.mock.firebolt.io")]
        [TestCase("database=testdb.ib;clientid=testuser;account=accountname;endpoint=api.mock.firebolt.io")]
        public void ParsingMissClientSecretConnectionStringTest(string connectionString)
        {
            var ex = Throws<FireboltException>(
                delegate { new FireboltConnection(connectionString); });
            That(ex?.Message, Is.EqualTo("ClientSecret parameter is missing in the connection string"));
        }

        [TestCase("database=testdb.ib;clientid=;clientsecret=testpwd;account=accountname;endpoint=api.mock.firebolt.io")]
        [TestCase("database=testdb.ib;clientsecret=testpwd;account=accountname;endpoint=api.mock.firebolt.io")]
        public void ParsingMissClientIdConnectionStringTest(string connectionString)
        {
            var ex = Throws<FireboltException>(
                delegate { new FireboltConnection(connectionString); });
            That(ex?.Message, Is.EqualTo("ClientId parameter is missing in the connection string"));
        }

        [TestCase("database=testdb.ib;clientid=testuser;clientsecret=testpwd;account=;endpoint=api.mock.firebolt.io")]
        [TestCase("database=testdb.ib;clientid=testuser;clientsecret=testpwd;endpoint=api.mock.firebolt.io")]
        public void ParsingMissAccountConnectionStringTest(string connectionString)
        {
            var ex = Throws<FireboltException>(
                delegate { new FireboltConnection(connectionString); });
            That(ex?.Message, Is.EqualTo("Account parameter is missing in the connection string"));
        }

        [Test]
        public void ParsingInvalidConnectionStringTest()
        {
            const string connectionString = "database=testdb.ib;clientid=test_user;clientsecret=test_pwd;account=account_name;endpoint=api.mock.firebolt.io";
            var cs = new FireboltConnection(connectionString);
            Multiple(() =>
            {
                That(cs.Endpoint, Is.EqualTo("api.mock.firebolt.io"));
                That(cs.Database, Is.EqualTo("testdb.ib"));
                That(cs.Account, Is.EqualTo("account_name"));
                That(cs.ClientSecret, Is.EqualTo("test_pwd"));
                That(cs.ClientId, Is.EqualTo("test_user"));
            });
        }

        [Test]
        public void OnSessionEstablishedTest()
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=testpwd;account=accountname;endpoint=api.mock.firebolt.io";
            var cs = new FireboltConnection(connectionString);
            cs.OnSessionEstablished();
            That(cs.State, Is.EqualTo(ConnectionState.Open));
        }

        [Test]
        public void CloseTest()
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=test_pwd;account=accountname;endpoint=api.mock.firebolt.io";
            var cs = new FireboltConnection(connectionString);
            var conState = new FireboltConnectionState();
            cs.Close();
            That(conState.State, Is.EqualTo(ConnectionState.Closed));
            That(cs.Client, Is.EqualTo(null));
        }

        [TestCase("test")]
        public void ParsingDatabaseHostnames(string hostname)
        {
            var ConnectionString = $"database={hostname}:test.ib;clientid=user;clientid=testuser;clientsecret=password;account=accountname;endpoint=api.mock.firebolt.io";
            var cs = new FireboltConnection(ConnectionString);
            That(cs.Database, Is.EqualTo("test:test.ib"));
        }

        [Test]
        public void OpenExceptionTest()
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=password;account=accountname;endpoint=api.mock.firebolt.io";
            var cs = new FireboltConnection(connectionString);
            var ex = ThrowsAsync<HttpRequestException>(async () => await cs.OpenAsync());
        }

        [Test]
        public void OpenInvalidUrlTest()
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=passwordtest;account=accountname;endpoint=api.mock.firebolt.io";
            var cs = new FireboltConnection(connectionString);
            HttpRequestException? exception = Throws<HttpRequestException>(() => cs.Open());
            Assert.NotNull(exception);
            That(exception!.Message, Is.EqualTo("Name or service not known (id.mock.firebolt.io:443)"));
        }

        [Test]
        public void CreateCursorTest()
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=passwordtest;account=accountname;endpoint=api.mock.firebolt.io;";
            var cs = new FireboltConnection(connectionString);
            var cursor = cs.CreateCursor();
            Equals("testdb.ib", cursor.Connection?.Database);
        }

        [TestCase("Select 1")]
        public void CreateCursorCommandTextTest(string commandText)
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=passwordtest;account=accountname;endpoint=api.mock.firebolt.io";
            var cs = new FireboltConnection(connectionString);
            var cursor = cs.CreateCursor(commandText);
            That(cursor.CommandText, Is.EqualTo("Select 1"));
        }

        [TestCase("Select 1")]
        public void FireboltExceptionTest(string commandText)
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=passwordtest;account=accountname;endpoint=api.mock.firebolt.io";
            var cs = new FireboltConnection(connectionString);
            var cursor = cs.CreateCursor(commandText);
            That(cursor.CommandText, Is.EqualTo("Select 1"));
        }

    }
}
