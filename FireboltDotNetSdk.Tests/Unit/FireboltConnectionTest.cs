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
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=testpwd;account=accountname;endpoint=endpoint";
            var cs = new FireboltConnection(connectionString);
            Multiple(() =>
            {
                That(cs.Endpoint, Is.EqualTo("endpoint"));
                That(cs.Database, Is.EqualTo("testdb.ib"));
                That(cs.Account, Is.EqualTo("accountname"));
                That(cs.ClientSecret, Is.EqualTo("testpwd"));
                That(cs.ClientId, Is.EqualTo("testuser"));
            });
        }

        [TestCase("database=testdb.ib;clientid=testuser;clientsecret=;account=accountname;endpoint=endpoint")]
        [TestCase("database=testdb.ib;clientid=testuser;account=accountname;endpoint=endpoint")]
        public void ParsingMissClientSecretConnectionStringTest(string connectionString)
        {
            var ex = Throws<FireboltException>(
                delegate { new FireboltConnection(connectionString); });
            That(ex?.Message, Is.EqualTo("ClientSecret parameter is missing in the connection string"));
        }

        [TestCase("database=testdb.ib;clientid=;clientsecret=testpwd;account=accountname;endpoint=endpoint")]
        [TestCase("database=testdb.ib;clientsecret=testpwd;account=accountname;endpoint=endpoint")]
        public void ParsingMissClientIdConnectionStringTest(string connectionString)
        {
            var ex = Throws<FireboltException>(
                delegate { new FireboltConnection(connectionString); });
            That(ex?.Message, Is.EqualTo("ClientId parameter is missing in the connection string"));
        }

        [TestCase("database=testdb.ib;clientid=testuser;clientsecret=testpwd;account=;endpoint=endpoint")]
        [TestCase("database=testdb.ib;clientid=testuser;clientsecret=testpwd;endpoint=endpoint")]
        public void ParsingMissAccountConnectionStringTest(string connectionString)
        {
            var ex = Throws<FireboltException>(
                delegate { new FireboltConnection(connectionString); });
            That(ex?.Message, Is.EqualTo("Account parameter is missing in the connection string"));
        }

        [Test]
        public void ParsingInvalidConnectionStringTest()
        {
            const string connectionString = "database=testdb.ib;clientid=test_user;clientsecret=test_pwd;account=account_name;endpoint=endpoint";
            var cs = new FireboltConnection(connectionString);
            Multiple(() =>
            {
                That(cs.Endpoint, Is.EqualTo("endpoint"));
                That(cs.Database, Is.EqualTo("testdb.ib"));
                That(cs.Account, Is.EqualTo("account_name"));
                That(cs.ClientSecret, Is.EqualTo("test_pwd"));
                That(cs.ClientId, Is.EqualTo("test_user"));
            });
        }

        [Test]
        public void OnSessionEstablishedTest()
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=testpwd;account=accountname;endpoint=endpoint";
            var cs = new FireboltConnection(connectionString);
            cs.OnSessionEstablished();
            That(cs.State, Is.EqualTo(ConnectionState.Open));
        }

        [Test]
        public void CloseTest()
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=test_pwd;account=accountname;endpoint=endpoint";
            var cs = new FireboltConnection(connectionString);
            var conState = new FireboltConnectionState();
            cs.Close();
            That(conState.State, Is.EqualTo(ConnectionState.Closed));
            That(cs.Client, Is.EqualTo(null));
        }

        [TestCase("test")]
        public void ParsingDatabaseHostnames(string hostname)
        {
            var ConnectionString = $"database={hostname}:test.ib;clientid=user;clientid=testuser;clientsecret=password;account=accountname;endpoint=endpoint";
            var cs = new FireboltConnection(ConnectionString);
            That(cs.Database, Is.EqualTo("test:test.ib"));
        }

        [Test]
        public void OpenExceptionTest()
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=password;account=accountname;endpoint=endpoint";
            var cs = new FireboltConnection(connectionString);
            var ex = ThrowsAsync<InvalidOperationException>(async () => await cs.OpenAsync());
        }


        [Test]
        public void OpenAsyncTest()
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=password;account=accountname;endpoint=endpoint";
            var cs = new FireboltConnection(connectionString);
            InvalidOperationException? exception = ThrowsAsync<InvalidOperationException>(() => cs.OpenAsync());
            Assert.NotNull(exception);
            That(exception!.Message, Is.EqualTo("An invalid request URI was provided. Either the request URI must be an absolute URI or BaseAddress must be set."));
        }

        [Test]
        public void OpenInvalidUrlTest()
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=passwordtest;account=accountname;endpoint=endpoint";
            var cs = new FireboltConnection(connectionString);
            InvalidOperationException? exception = Throws<InvalidOperationException>(() => cs.Open());
            Assert.NotNull(exception);
            That(exception!.Message, Is.EqualTo("An invalid request URI was provided. Either the request URI must be an absolute URI or BaseAddress must be set."));
        }

        [Test]
        public void CreateCursorTest()
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=passwordtest;account=accountname;endpoint=endpoint;";
            var cs = new FireboltConnection(connectionString);
            var cursor = cs.CreateCursor();
            Equals("testdb.ib", cursor.Connection?.Database);
        }

        [TestCase("Select 1")]
        public void CreateCursorCommandTextTest(string commandText)
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=passwordtest;account=accountname;endpoint=endpoint";
            var cs = new FireboltConnection(connectionString);
            var cursor = cs.CreateCursor(commandText);
            That(cursor.CommandText, Is.EqualTo("Select 1"));
        }

        [TestCase("Select 1")]
        public void FireboltExceptionTest(string commandText)
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=passwordtest;account=accountname;endpoint=endpoint";
            var cs = new FireboltConnection(connectionString);
            var cursor = cs.CreateCursor(commandText);
            That(cursor.CommandText, Is.EqualTo("Select 1"));
        }

    }
}
