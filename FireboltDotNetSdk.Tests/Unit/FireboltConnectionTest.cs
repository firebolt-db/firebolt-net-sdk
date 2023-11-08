using System.Data;
using System.Data.Common;
using System.Net;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;
using Moq;
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
            public override async Task<bool> OpenAsync(CancellationToken cancellationToken)
            {
                _openedWithConnectionString = ConnectionString;
                return await Task.FromResult(true);
            }
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
                That(cs.DataSource, Is.EqualTo("testdb.ib"));
                That(cs.Account, Is.EqualTo("accountname"));
                That(cs.Secret, Is.EqualTo("testpwd"));
                That(cs.Principal, Is.EqualTo("testuser"));
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
                That(cs.Secret, Is.EqualTo("testpwd"));
                That(cs.Principal, Is.EqualTo("testuser"));
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
                That(cs.Secret, Is.EqualTo("testpwd"));
                That(cs.Principal, Is.EqualTo("testuser"));
            });
        }

        [TestCase("database=testdb.ib;clientid=testuser;clientsecret=;account=accountname;endpoint=api.mock.firebolt.io")]
        [TestCase("database=testdb.ib;clientid=testuser;account=accountname;endpoint=api.mock.firebolt.io")]
        public void ParsingMissClientSecretConnectionStringTest(string connectionString)
        {
            var ex = Throws<FireboltException>(
                delegate { new FireboltConnection(connectionString); });
            That(ex?.Message, Is.EqualTo("Either Password or ClientSecret parameter is missing in the connection string"));
        }

        [TestCase("database=testdb.ib;clientid=;clientsecret=testpwd;account=accountname;endpoint=api.mock.firebolt.io")]
        [TestCase("database=testdb.ib;clientsecret=testpwd;account=accountname;endpoint=api.mock.firebolt.io")]
        public void ParsingMissClientIdConnectionStringTest(string connectionString)
        {
            var ex = Throws<FireboltException>(
                delegate { new FireboltConnection(connectionString); });
            That(ex?.Message, Is.EqualTo("Either UserName or ClientId parameter is missing in the connection string"));
        }

        [TestCase("database=testdb.ib;clientid=client;username=user;clientsecret=testpwd;account=accountname;endpoint=api.mock.firebolt.io")]
        public void ParsingClientIdAndUserNameConnectionStringTest(string connectionString)
        {
            var ex = Throws<FireboltException>(delegate { new FireboltConnection(connectionString); });
            That(ex?.Message, Is.EqualTo("Ambiguous values of UserName and ClientId. Use only one of them"));
        }

        [TestCase("database=testdb.ib;clientid=client;password=testpwd;clientsecret=testpwd;account=accountname;endpoint=api.mock.firebolt.io")]
        [TestCase("database=testdb.ib;username=client;password=testpwd;clientsecret=testpwd;account=accountname;endpoint=api.mock.firebolt.io")]
        public void ParsingClientSecretAndPasswordConnectionStringTest(string connectionString)
        {
            var ex = Throws<FireboltException>(delegate { new FireboltConnection(connectionString); });
            That(ex?.Message, Is.EqualTo("Ambiguous values of Password and ClientSecret. Use only one of them"));
        }

        [TestCase("database=testdb.ib;clientid=testuser;clientsecret=testpwd;account=")]
        [TestCase("database=testdb.ib;clientid=testuser;clientsecret=testpwd")]
        public void ParsingMissAccountConnectionStringTestV2(string connectionString)
        {
            var ex = Throws<FireboltException>(
                delegate { new FireboltConnection(connectionString); });
            That(ex?.Message, Is.EqualTo("Account parameter is missing in the connection string"));
        }

        [TestCase("database=testdb.ib;clientid=testuser;clientsecret=testpwd;account=;endpoint=api.mock.firebolt.io")]
        [TestCase("database=testdb.ib;clientid=testuser;clientsecret=testpwd;endpoint=api.mock.firebolt.io")]
        public void ParsingMissAccountConnectionStringTestV1(string connectionString)
        {
            IsNotNull(new FireboltConnection(connectionString)); // V1 does not require account and should succeed
            // IsNotNull is always true here and needed just to satisfy Sonar.
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
                That(cs.Secret, Is.EqualTo("test_pwd"));
                That(cs.Principal, Is.EqualTo("test_user"));
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
            Mock<HttpClient> httpClientMock = new();
            var cs = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient2(cs, Guid.NewGuid().ToString(), "any", "test.api.firebolt.io", null, "account", httpClientMock.Object);
            cs.Client = client;
            That(client, Is.SameAs(cs.Client));
            cs.Close();
            That(cs.State, Is.EqualTo(ConnectionState.Closed));
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
        public void SetDifferentConnectionStringWithSameParameters()
        {
            const string connectionString1 = "clientid=testuser;clientsecret=testpwd;account=account1;engine=diesel;database=db;env=test";
            var cs = new MockFireboltConnection(connectionString1);
            That(cs.OpenedWithConnectionString, Is.EqualTo(null));
            cs.Open();
            That(cs.OpenedWithConnectionString, Is.EqualTo(connectionString1));
            That(cs.ClosedWithConnectionString, Is.EqualTo(null));

            const string connectionString2 = "account=account1;engine=diesel;database=db;env=test;clientid=testuser;clientsecret=testpwd";
            cs.ConnectionString = connectionString2;
            // The connection was not re-opened
            That(cs.ClosedWithConnectionString, Is.EqualTo(null));
            That(cs.OpenedWithConnectionString, Is.EqualTo(connectionString1));
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
        public async Task SetSameDatabaseAsync()
        {
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname;database=db";
            var cs = new MockFireboltConnection(connectionString);
            That(cs.OpenedWithConnectionString, Is.EqualTo(null));
            cs.Open();
            That(cs.OpenedWithConnectionString, Is.EqualTo(connectionString));
            That(cs.ClosedWithConnectionString, Is.EqualTo(null));

            cs.Reset();

            await cs.ChangeDatabaseAsync("db");
            That(cs.ClosedWithConnectionString, Is.EqualTo(null));
            That(cs.OpenedWithConnectionString, Is.EqualTo(null));

            cs.Close();
            That(cs.ClosedWithConnectionString, Is.EqualTo(connectionString));
        }

        [Test]
        public void SetDifferentDatabaseAsync()
        {
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname;database=one";
            var cs = new MockFireboltConnection(connectionString);
            That(cs.OpenedWithConnectionString, Is.EqualTo(null));
            cs.Open();
            That(cs.OpenedWithConnectionString, Is.EqualTo(connectionString));
            That(cs.ClosedWithConnectionString, Is.EqualTo(null));

            cs.Reset();

            cs.ChangeDatabaseAsync("two");
            string connectionString2 = "clientid=testuser;clientsecret=testpwd;account=accountname;database=two";
            That(cs.ClosedWithConnectionString, Is.EqualTo(connectionString));
            That(cs.OpenedWithConnectionString, Is.EqualTo(connectionString2));

            cs.Close();
            That(cs.ClosedWithConnectionString, Is.EqualTo(connectionString2));
        }

        [Test]
        public void FakeTransaction()
        {
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname;engine=diesel";
            var cs = new FireboltConnection(connectionString);
            DbTransaction transaction = cs.BeginTransaction();
            NotNull(transaction);
            False(transaction.SupportsSavepoints);
            That(transaction.Connection, Is.SameAs(cs));
            That(transaction.IsolationLevel, Is.EqualTo(IsolationLevel.Unspecified));
            transaction.Commit();
            Throws<NotImplementedException>(() => transaction.Rollback());
        }

        [Test]
        public void NotImplementedMethods()
        {
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname;engine=diesel";
            var cs = new FireboltConnection(connectionString);
            Throws<NotSupportedException>(() => cs.EnlistTransaction(null));
            Throws<NotSupportedException>(() => cs.EnlistTransaction(new Mock<System.Transactions.Transaction>().Object));
            Throws<NotImplementedException>(() => cs.GetSchema());
            Throws<NotImplementedException>(() => cs.GetSchema("collection"));
            Throws<NotImplementedException>(() => cs.GetSchema("collection", new string[0]));
        }

        [Test]
        public void WrongAuth()
        {
            Mock<HttpClient> httpClientMock = new();
            httpClientMock.Setup(m => m.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>())).Throws<HttpRequestException>();
            const string connectionString = "clientid=wrong;clientsecret=wrong;account=accountname;engine=diesel";
            var cs = new FireboltConnection(connectionString);
            var client = new FireboltClient2(cs, "wrong", "wrong", "", "test", "account", httpClientMock.Object);
            cs.Client = client;
            Throws<HttpRequestException>(() => cs.Open());
            ThrowsAsync<HttpRequestException>(async () => await cs.OpenAsync());
        }

        [Test]
        public void SuccessfulLogin()
        {
            Mock<HttpClient> httpClientMock = new Mock<HttpClient>();
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname";
            var cs = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient2(cs, Guid.NewGuid().ToString(), "password", "", "test", "account", httpClientMock.Object);
            cs.Client = client;
            FireResponse.LoginResponse loginResponse = new FireResponse.LoginResponse("access_token", "3600", "Bearer");
            FireResponse.GetSystemEngineUrlResponse systemEngineResponse = new FireResponse.GetSystemEngineUrlResponse() { engineUrl = "api.test.firebolt.io" };
            httpClientMock.SetupSequence(p => p.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(FireboltClientTest.GetResponseMessage(loginResponse, HttpStatusCode.OK)) // retrieve access token
            .ReturnsAsync(FireboltClientTest.GetResponseMessage(systemEngineResponse, HttpStatusCode.OK)) // get system engine URL
            ;
            // const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname";
            // var cs = new FireboltConnection(connectionString) { Client = client };
            cs.Open(); // should succeed
            // Due to Open does not return value the only way to validate that everything passed well is to validate that SendAsync was called exactly twice:
            // 1. to retrive token
            // 2. to retrieve system engine URL
            httpClientMock.Verify(m => m.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
        }

        [Test]
        public void SuccessfulLoginWithEngineName()
        {
            Mock<HttpClient> httpClientMock = new Mock<HttpClient>();
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname;engine=diesel";
            var cs = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient2(cs, Guid.NewGuid().ToString(), "password", "", "test", "account", httpClientMock.Object);
            FireResponse.LoginResponse loginResponse = new FireResponse.LoginResponse("access_token", "3600", "Bearer");
            FireResponse.GetSystemEngineUrlResponse systemEngineResponse = new FireResponse.GetSystemEngineUrlResponse() { engineUrl = "api.test.firebolt.io" };
            FireResponse.GetAccountIdByNameResponse accountIdResponse = new FireResponse.GetAccountIdByNameResponse() { id = "account_id" };
            string engineUrlMeta = "\"meta\":[{\"name\": \"url\", \"type\": \"string\"}, {\"name\": \"attached_to\", \"type\": \"string\"}, {\"name\": \"uint8\", \"type\": \"string\"}, {\"name\": \"database_name\", \"type\": \"string\"}, {\"name\": \"status\"}]";
            string engineUrlData = "\"data\":[[\"api.firebolt.io\", \"db\", \"diesel\", \"RUNNING\"]]";
            httpClientMock.SetupSequence(p => p.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(FireboltClientTest.GetResponseMessage(loginResponse, HttpStatusCode.OK)) // retrieve access token
            .ReturnsAsync(FireboltClientTest.GetResponseMessage(systemEngineResponse, HttpStatusCode.OK)) // get system engine URL
            .ReturnsAsync(FireboltClientTest.GetResponseMessage(accountIdResponse, HttpStatusCode.OK)) // get account ID
            .ReturnsAsync(FireboltClientTest.GetResponseMessage("{\"query\":{\"query_id\": \"1\"},\"meta\":[{\"type\": \"text\", \"name\": \"attached_to\"}],\"data\":[[\"db\"]]}", HttpStatusCode.OK)) // get Engine DB
            .ReturnsAsync(FireboltClientTest.GetResponseMessage("{\"query\":{\"query_id\": \"2\"},\"meta\":[{\"type\": \"t\", \"name\": \"database_name\"}],\"data\":[[\"db\"]]}", HttpStatusCode.OK)) // check whether the DB is accessible
            .ReturnsAsync(FireboltClientTest.GetResponseMessage("{\"query\":{\"query_id\": \"3\"}," + engineUrlMeta + ", " + engineUrlData + "}", HttpStatusCode.OK)) // get engine URL
            .ReturnsAsync(FireboltClientTest.GetResponseMessage("{\"query\":{\"query_id\": \"1\"},\"meta\":[{\"type\": \"string\"}, {\"name\": \"version()\"}],\"data\":[[\"1.2.3\"]]}", HttpStatusCode.OK)) // get version
            ;
            cs.Client = client;
            cs.Open(); // should succeed
            That(cs.ServerVersion, Is.EqualTo("1.2.3"));
            httpClientMock.Verify(m => m.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()), Times.Exactly(7));
        }

        [Test]
        public void SuccessfulLoginWithEngineNameDbNotAccessible()
        {
            Mock<HttpClient> httpClientMock = new Mock<HttpClient>();
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname;engine=diesel;database=db";
            var cs = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient2(cs, Guid.NewGuid().ToString(), "password", "", "test", "account", httpClientMock.Object);
            FireResponse.LoginResponse loginResponse = new FireResponse.LoginResponse("access_token", "3600", "Bearer");
            FireResponse.GetSystemEngineUrlResponse systemEngineResponse = new FireResponse.GetSystemEngineUrlResponse() { engineUrl = "api.test.firebolt.io" };
            FireResponse.GetAccountIdByNameResponse accountIdResponse = new FireResponse.GetAccountIdByNameResponse() { id = "account_id" };
            httpClientMock.SetupSequence(p => p.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(FireboltClientTest.GetResponseMessage(loginResponse, HttpStatusCode.OK)) // retrieve access token
            .ReturnsAsync(FireboltClientTest.GetResponseMessage(systemEngineResponse, HttpStatusCode.OK)) // get system engine URL
            .ReturnsAsync(FireboltClientTest.GetResponseMessage(accountIdResponse, HttpStatusCode.OK)) // get account ID
            .ReturnsAsync(FireboltClientTest.GetResponseMessage("{\"query\":{\"query_id\": \"2\"},\"meta\":[{\"name\": \"uint8\"}, {\"name\": \"database_name\"}],\"data\":[]}", HttpStatusCode.OK)) // check whether the DB is accessible - no
            ;
            cs.Client = client;
            That(Throws<FireboltException>(() => cs.Open())?.Message, Is.EqualTo("Database db does not exist or current user does not have access to it!"));
            httpClientMock.Verify(m => m.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()), Times.Exactly(4));
        }

        [TestCase("\"data\":[]", "Engine diesel not found.")]
        [TestCase("\"data\":[[\"api.firebolt.io\", null, \"diesel\", \"RUNNING\"]]", "Engine diesel is not attached to any database")]
        [TestCase("\"data\":[[\"api.firebolt.io\", \"other_db\", \"diesel\", \"RUNNING\"]]", "Engine diesel is not attached to db")]
        [TestCase("\"data\":[[\"api.firebolt.io\", \"db\", \"diesel\", \"STOPPED\"]]", "Engine diesel is not running")]
        [TestCase("\"data\":[[\"api.firebolt.io\", \"db\", \"diesel\", \"RUNNING\"], [\"api.firebolt.io\", \"db\", \"diesel\", \"RUNNING\"]]", "Unexpected duplicate entries found for diesel and database db")]
        public void LoginFailedOnEngineUrl(string engineUrlData, string expectedErrorMessage)
        {
            Mock<HttpClient> httpClientMock = new Mock<HttpClient>();
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname;engine=diesel";
            var cs = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient2(cs, Guid.NewGuid().ToString(), "password", "", "test", "account", httpClientMock.Object);
            cs.Client = client;
            FireResponse.LoginResponse loginResponse = new FireResponse.LoginResponse("access_token", "3600", "Bearer");
            FireResponse.GetSystemEngineUrlResponse systemEngineResponse = new FireResponse.GetSystemEngineUrlResponse() { engineUrl = "api.test.firebolt.io" };
            FireResponse.GetAccountIdByNameResponse accountIdResponse = new FireResponse.GetAccountIdByNameResponse() { id = "account_id" };
            string engineUrlMeta = "\"meta\":[{\"name\": \"url\", \"type\": \"string\"}, {\"name\": \"attached_to\", \"type\": \"string\"}, {\"name\": \"uint8\", \"type\": \"string\"}, {\"name\": \"database_name\", \"type\": \"string\"}, {\"name\": \"status\"}]";
            httpClientMock.SetupSequence(p => p.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(FireboltClientTest.GetResponseMessage(loginResponse, HttpStatusCode.OK)) // retrieve access token
            .ReturnsAsync(FireboltClientTest.GetResponseMessage(systemEngineResponse, HttpStatusCode.OK)) // get system engine URL
            .ReturnsAsync(FireboltClientTest.GetResponseMessage(accountIdResponse, HttpStatusCode.OK)) // get account ID
            .ReturnsAsync(FireboltClientTest.GetResponseMessage("{\"query\":{\"query_id\": \"1\"},\"meta\":[{\"type\": \"text\", \"name\": \"attached_to\"}],\"data\":[[\"db\"]]}", HttpStatusCode.OK)) // get Engine DB
            .ReturnsAsync(FireboltClientTest.GetResponseMessage("{\"query\":{\"query_id\": \"2\"},\"meta\":[{\"type\": \"text\", \"name\": \"database_name\"}],\"data\":[[\"db\"]]}", HttpStatusCode.OK)) // check whether the DB is accessible
            .ReturnsAsync(FireboltClientTest.GetResponseMessage("{\"query\":{\"query_id\": \"3\"}," + engineUrlMeta + ", " + engineUrlData + "}", HttpStatusCode.OK)) // get engine URL
            ;
            That(Throws<FireboltException>(() => cs.Open())?.Message, Is.EqualTo(expectedErrorMessage));
            httpClientMock.Verify(m => m.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()), Times.Exactly(6));
        }
    }
}