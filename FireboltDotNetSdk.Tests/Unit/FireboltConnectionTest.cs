using System.Data;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;
using static NUnit.Framework.Assert;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    public class FireboltConnectionTest
    {
        private class MockFireboltConnection : FireboltConnection
        {
            private string? _openedWithConnectionString;
            private string? _closedWithConnectionString;

            public MockFireboltConnection(string connectionString) : base(connectionString) { Client = new MockClient(null); }

            public string? OpenedWithConnectionString { get => _openedWithConnectionString; }
            public string? ClosedWithConnectionString { get => _closedWithConnectionString; }

            public override void Open() { _openedWithConnectionString = ConnectionString; }

            public override void Close() { _closedWithConnectionString = ConnectionString; }

            public void Reset()
            {
                _openedWithConnectionString = null;
                _closedWithConnectionString = null;
            }
        }

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

        [Test]
        public void ParsingNoEndpointEnvConnectionStringTest()
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=testpwd;account=accountname;endpoint=some.weird.endpoint;env=mock";
            var cs = new FireboltConnection(connectionString);
            Multiple(() =>
            {
                That(cs.Endpoint, Is.EqualTo("some.weird.endpoint"));
                That(cs.Env, Is.EqualTo("mock"));
                That(cs.Database, Is.EqualTo("testdb.ib"));
                That(cs.Account, Is.EqualTo("accountname"));
                That(cs.ClientSecret, Is.EqualTo("testpwd"));
                That(cs.ClientId, Is.EqualTo("testuser"));
            });
        }

        [Test]
        public void ParsingEngineConnectionStringTest()
        {
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname;engine=diesel";
            var cs = new FireboltConnection(connectionString);
            Multiple(() =>
            {
                That(cs.Endpoint, Is.EqualTo(Constant.DEFAULT_ENDPOINT));
                That(cs.Env, Is.EqualTo(Constant.DEFAULT_ENV));
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
        public void ParsingIncompatibleEndpointAndEnvTest()
        {
            const string connectionString = "database=testdb.ib;clientid=test_user;clientsecret=test_pwd;account=account_name;endpoint=api.mock.firebolt.io;env=mock2";
            var ex = Throws<FireboltException>(
                    delegate { new FireboltConnection(connectionString); });
            That(ex?.Message, Is.EqualTo("Configuration error: environment mock2 and endpoint api.mock.firebolt.io are incompatible"));
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
        public void CreateCommandTest()
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=passwordtest;account=accountname;endpoint=api.mock.firebolt.io;";
            var cs = new FireboltConnection(connectionString);
            var command = cs.CreateCommand();
            Equals("testdb.ib", command.Connection?.Database);
        }

        [TestCase("Select 1")]
        public void CreateCommandTextTest(string commandText)
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=passwordtest;account=accountname;endpoint=api.mock.firebolt.io";
            var cs = new FireboltConnection(connectionString);
            var command = cs.CreateCommand();
            command.CommandText = commandText;
            That(command.CommandText, Is.EqualTo("Select 1"));
        }

        [Test]
        public void CreateConnectionWithNullConnectionString()
        {
            var cs = new FireboltConnection("clientid=testuser;clientsecret=testpwd;account=accountname;engine=my");
            Throws<ArgumentNullException>(() => cs.ConnectionString = null);
        }

        [Test]
        public void SetSameConnectionString()
        {
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname;engine=my";
            var cs = new MockFireboltConnection(connectionString);
            That(cs.OpenedWithConnectionString, Is.EqualTo(null));
            cs.Open();
            That(cs.OpenedWithConnectionString, Is.EqualTo(connectionString));
            That(cs.ClosedWithConnectionString, Is.EqualTo(null));

            cs.Reset();

            cs.ConnectionString = connectionString;
            That(cs.ClosedWithConnectionString, Is.EqualTo(null));
            That(cs.OpenedWithConnectionString, Is.EqualTo(null));

            cs.Close();
            That(cs.ClosedWithConnectionString, Is.EqualTo(connectionString));
        }

        [Test]
        public void SetDifferentConnectionString()
        {
            const string connectionString1 = "clientid=testuser;clientsecret=testpwd;account=account1;engine=diesel";
            var cs = new MockFireboltConnection(connectionString1);
            That(cs.OpenedWithConnectionString, Is.EqualTo(null));
            cs.Open();
            That(cs.OpenedWithConnectionString, Is.EqualTo(connectionString1));
            That(cs.ClosedWithConnectionString, Is.EqualTo(null));

            const string connectionString2 = "clientid=testuser;clientsecret=testpwd;account=account2;engine=benzene";

            cs.ConnectionString = connectionString2;
            That(cs.ClosedWithConnectionString, Is.EqualTo(connectionString1));
            That(cs.OpenedWithConnectionString, Is.EqualTo(connectionString2));
            That(cs.Account, Is.EqualTo("account2"));
        }

        [Test]
        public void SetSameDatabase()
        {
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname;database=db";
            var cs = new MockFireboltConnection(connectionString);
            That(cs.OpenedWithConnectionString, Is.EqualTo(null));
            cs.Open();
            That(cs.OpenedWithConnectionString, Is.EqualTo(connectionString));
            That(cs.ClosedWithConnectionString, Is.EqualTo(null));

            cs.Reset();

            cs.ChangeDatabase("db");
            That(cs.ClosedWithConnectionString, Is.EqualTo(null));
            That(cs.OpenedWithConnectionString, Is.EqualTo(null));

            cs.Close();
            That(cs.ClosedWithConnectionString, Is.EqualTo(connectionString));
        }

        [Test]
        public void SetDifferentDatabase()
        {
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname;database=one";
            var cs = new MockFireboltConnection(connectionString);
            That(cs.OpenedWithConnectionString, Is.EqualTo(null));
            cs.Open();
            That(cs.OpenedWithConnectionString, Is.EqualTo(connectionString));
            That(cs.ClosedWithConnectionString, Is.EqualTo(null));

            cs.Reset();

            cs.ChangeDatabase("two");
            string connectionString2 = "clientid=testuser;clientsecret=testpwd;account=accountname;database=two";
            That(cs.ClosedWithConnectionString, Is.EqualTo(connectionString));
            That(cs.OpenedWithConnectionString, Is.EqualTo(connectionString2));

            cs.Close();
            That(cs.ClosedWithConnectionString, Is.EqualTo(connectionString2));
        }

        [Test]
        public void NotImplementedMethods()
        {
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname;engine=diesel";
            var cs = new FireboltConnection(connectionString);
            Throws<NotImplementedException>(() => cs.BeginTransaction());
            Throws<NotSupportedException>(() => cs.EnlistTransaction(null));
            Throws<NotImplementedException>(() => cs.GetSchema());
            Throws<NotImplementedException>(() => cs.GetSchema("collection"));
            Throws<NotImplementedException>(() => cs.GetSchema("collection", new string[0]));
        }
    }
}
