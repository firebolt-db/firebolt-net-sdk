using System.Data;
using System.Data.Common;
using System.Net;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;
using Moq;
using Org.BouncyCastle.Utilities;
using static NUnit.Framework.Assert;
using Times = Moq.Times;

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

            public DbProviderFactory GetDbProviderFactory()
            {
                return base.DbProviderFactory;
            }

        }

        [Test]
        public void ConnectionAccountId()
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=testpwd;account=accountname";
            var cs = new FireboltConnection(connectionString);
            cs.AccountId = "a123";
            Assert.That(cs.AccountId, Is.EqualTo("a123"));
            Assert.That(cs.InfraVersion, Is.EqualTo(1));
        }

        [Test]
        public void ConnectionInfraVersion()
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=testpwd;account=accountname";
            var cs = new MockFireboltConnection(connectionString);
            cs.CleanupCache();
            Assert.Null(cs.AccountId); // retrieving account ID initializes the InfraVersion that is 1 by default
            Assert.That(cs.InfraVersion, Is.EqualTo(1));
            // now let's change the InfraVersion
            cs.InfraVersion = 5;
            Assert.That(cs.InfraVersion, Is.EqualTo(5));
        }

        [Test]
        public void ConnectionDbProviderFactory()
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=testpwd;account=accountname";
            var cs = new MockFireboltConnection(connectionString);
            Assert.That(cs.GetDbProviderFactory().GetType(), Is.EqualTo(typeof(FireboltClientFactory)));
        }

        [Test]
        public void UpdateConnectionSettings()
        {
            var cs = new FireboltConnection("database=db1;clientid=id;clientsecret=secret;account=a1;engine=e1");
            Multiple(() =>
            {
                That(cs.Database, Is.EqualTo("db1"));
                That(cs.DataSource, Is.EqualTo("db1"));
                That(cs.EngineName, Is.EqualTo("e1"));
            });
            cs.UpdateConnectionSettings(new FireboltConnectionStringBuilder("database=db2;username=usr;password=pwd;account=a2;engine=e2"), CancellationToken.None);
            Multiple(() =>
            {
                That(cs.Database, Is.EqualTo("db2"));
                That(cs.DataSource, Is.EqualTo("db2"));
                That(cs.EngineName, Is.EqualTo("e2"));
            });
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
        [TestCase("database=testdb.ib;clientid=client;password=testpwd;clientsecret=testpwd;account=accountname;endpoint=api.mock.firebolt.io")]
        [TestCase("database=testdb.ib;username=client;password=testpwd;clientsecret=testpwd;account=accountname;endpoint=api.mock.firebolt.io")]
        public void ParsingWrongPasswordAndClientSecretConnectionStringTest(string connectionString)
        {
            ParsingWrongConnectionStringTestV2(connectionString, "Configuration error: either Password or ClientSecret must be provided but not both");
        }

        [TestCase("database=testdb.ib;clientid=;clientsecret=testpwd;account=accountname;endpoint=api.mock.firebolt.io")]
        [TestCase("database=testdb.ib;clientsecret=testpwd;account=accountname;endpoint=api.mock.firebolt.io")]
        [TestCase("database=testdb.ib;clientid=client;username=user;clientsecret=testpwd;account=accountname;endpoint=api.mock.firebolt.io")]
        public void ParsingWrongUserNameAndClientIdConnectionStringTest(string connectionString)
        {
            ParsingWrongConnectionStringTestV2(connectionString, "Configuration error: either UserName or ClientId must be provided but not both");
        }

        [TestCase("database=testdb.ib;clientid=testuser;clientsecret=testpwd;account=")]
        [TestCase("database=testdb.ib;clientid=testuser;clientsecret=testpwd")]
        public void ParsingMissAccountConnectionStringTestV2(string connectionString)
        {
            ParsingWrongConnectionStringTestV2(connectionString, "Account parameter is missing in the connection string");
        }

        public void ParsingWrongConnectionStringTestV2(string connectionString, string expectedErrorMessage)
        {
            var ex = Throws<FireboltException>(delegate { new FireboltConnection(connectionString); });
            That(ex?.Message, Is.EqualTo(expectedErrorMessage));
        }

        [TestCase("database=testdb.ib;username=testuser@domain.com;password=testpwd;account=;endpoint=api.mock.firebolt.io")]
        [TestCase("database=testdb.ib;username=testuser@domain.com;password=testpwd;endpoint=api.mock.firebolt.io")]
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
            // Restore client since connection.Close() is called on error
            cs.Client = client;
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
            FireResponse.GetAccountIdByNameResponse accountIdResponse = new FireResponse.GetAccountIdByNameResponse() { id = "account_id" };
            httpClientMock.SetupSequence(p => p.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(FireboltClientTest.GetResponseMessage(loginResponse, HttpStatusCode.OK)) // retrieve access token
            .ReturnsAsync(FireboltClientTest.GetResponseMessage(systemEngineResponse, HttpStatusCode.OK)) // get system engine URL
            .ReturnsAsync(FireboltClientTest.GetResponseMessage("{\"query\":{\"query_id\": \"1\"},\"meta\":[{\"type\": \"int\", \"name\": \"1\"}],\"data\":[[\"1\"]]}", HttpStatusCode.OK)) // select 1 - to get infra version
            ;
            cs.CleanupCache();
            client.CleanupCache();
            cs.Open(); // should succeed
            // Due to Open does not return value the only way to validate that everything passed well is to validate that SendAsync was called exactly twice:
            // 1. to retrive token
            // 2. to retrieve system engine URL
            httpClientMock.Verify(m => m.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()), Times.Exactly(3));
        }

        [Test]
        public void SuccessfulLoginTwice()
        {
            // Simulate the first connection. Calls of CleanupCache() guarnatee that the global caches are empty
            Mock<HttpClient> httpClientMock1 = new Mock<HttpClient>();
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname";
            var cs1 = new FireboltConnection(connectionString);
            FireboltClient client1 = new FireboltClient2(cs1, Guid.NewGuid().ToString(), "password", "", "test", "account", httpClientMock1.Object);
            cs1.CleanupCache();
            client1.CleanupCache();

            cs1.Client = client1;
            FireResponse.LoginResponse loginResponse = new FireResponse.LoginResponse("access_token", "3600", "Bearer");
            FireResponse.GetSystemEngineUrlResponse systemEngineResponse = new FireResponse.GetSystemEngineUrlResponse() { engineUrl = "api.test.firebolt.io" };
            FireResponse.GetAccountIdByNameResponse accountIdResponse = new FireResponse.GetAccountIdByNameResponse() { id = "account_id" };
            httpClientMock1.SetupSequence(p => p.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(FireboltClientTest.GetResponseMessage(loginResponse, HttpStatusCode.OK)) // retrieve access token
            .ReturnsAsync(FireboltClientTest.GetResponseMessage(systemEngineResponse, HttpStatusCode.OK)) // get system engine URL
            .ReturnsAsync(FireboltClientTest.GetResponseMessage("{\"query\":{\"query_id\": \"1\"},\"meta\":[{\"type\": \"int\", \"name\": \"1\"}],\"data\":[[\"1\"]]}", HttpStatusCode.OK)) // select 1 - to get infra version
            ;
            cs1.Open(); // should succeed
            // Due to Open does not return value the only way to validate that everything passed well is to validate that SendAsync was called exactly twice:
            // 1. to retrive token
            // 2. to retrieve system engine URL
            httpClientMock1.Verify(m => m.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()), Times.Exactly(3));
            cs1.Close();

            // Now create the new connection. Due to caches are full we do not expect getting system engine URL and account ID. 
            var cs2 = new FireboltConnection(connectionString);
            Mock<HttpClient> httpClientMock2 = new Mock<HttpClient>();
            FireboltClient client2 = new FireboltClient2(cs2, Guid.NewGuid().ToString(), "password", "", "test", "account", httpClientMock2.Object);
            cs2.Client = client2;

            httpClientMock2.SetupSequence(p => p.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(FireboltClientTest.GetResponseMessage(loginResponse, HttpStatusCode.OK)) // retrieve access token
            .ReturnsAsync(FireboltClientTest.GetResponseMessage("{\"query\":{\"query_id\": \"1\"},\"meta\":[{\"type\": \"int\", \"name\": \"1\"}],\"data\":[[\"1\"]]}", HttpStatusCode.OK)) // select 1 - to get infra version
            ;
            cs2.Open(); // should succeed
            httpClientMock2.Verify(m => m.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
        }


        [TestCase("Running", "api.firebolt.io", "", "api.firebolt.io")]
        [TestCase("ENGINE_STATE_RUNNING", "api.firebolt.io?account_id=01hf9pchg0mnrd2g3hypm1dea4&engine=max_test", "", "api.firebolt.io")]
        public void SuccessfulLoginWithEngineName(string engineStatus, string engineUrl, string catalogs, string expectedEngineUrl)
        {
            Mock<HttpClient> httpClientMock = new Mock<HttpClient>();
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname;engine=diesel";
            var cs = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient2(cs, Guid.NewGuid().ToString(), "password", "", "test", "account", httpClientMock.Object);
            FireResponse.LoginResponse loginResponse = new FireResponse.LoginResponse("access_token", "3600", "Bearer");
            FireResponse.GetSystemEngineUrlResponse systemEngineResponse = new FireResponse.GetSystemEngineUrlResponse() { engineUrl = "api.test.firebolt.io" };
            FireResponse.GetAccountIdByNameResponse accountIdResponse = new FireResponse.GetAccountIdByNameResponse() { id = "account_id" };

            httpClientMock.SetupSequence(p => p.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(FireboltClientTest.GetResponseMessage(loginResponse, HttpStatusCode.OK)) // retrieve access token
            .ReturnsAsync(FireboltClientTest.GetResponseMessage(systemEngineResponse, HttpStatusCode.OK)) // get system engine URL
            .ReturnsAsync(FireboltClientTest.GetResponseMessage("{\"query\":{\"query_id\": \"3\"},\"meta\":[],\"data\":[]}", HttpStatusCode.OK)) // USE ENGINE
            .ReturnsAsync(FireboltClientTest.GetResponseMessage("{\"query\":{\"query_id\": \"1\"},\"meta\":[{\"type\": \"string\"}, {\"name\": \"version()\"}],\"data\":[[\"1.2.3\"]]}", HttpStatusCode.OK)) // get version
            ;

            cs.Client = client;
            cs.CleanupCache();
            client.CleanupCache();
            cs.Open(); // should succeed
            That(cs.ServerVersion, Is.EqualTo("1.2.3"));
            httpClientMock.Verify(m => m.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()), Times.Exactly(4));
        }

        /// <summary>
        /// Helper method to set up a FireboltConnection with mocked HttpClient for testing
        /// </summary>
        /// <param name="additionalResponses">Optional list of HttpResponseMessage objects to append after initial authentication</param>
        /// <returns>A tuple containing the FireboltConnection, Mock<HttpClient>, and FireboltClient</returns>
        private (FireboltConnection connection, Mock<HttpClient> httpClientMock, FireboltClient client) SetupFireboltConnection(params HttpResponseMessage[] additionalResponses)
        {
            Mock<HttpClient> httpClientMock = new Mock<HttpClient>();
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname";
            var connection = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient2(connection, Guid.NewGuid().ToString(), "password", "", "test", "account", httpClientMock.Object);
            connection.Client = client;

            FireResponse.LoginResponse loginResponse = new FireResponse.LoginResponse("access_token", "3600", "Bearer");
            FireResponse.GetSystemEngineUrlResponse systemEngineResponse = new FireResponse.GetSystemEngineUrlResponse() { engineUrl = "api.test.firebolt.io" };

            var setupSequence = httpClientMock.SetupSequence(p => p.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(FireboltClientTest.GetResponseMessage(loginResponse, HttpStatusCode.OK)) // retrieve access token
                .ReturnsAsync(FireboltClientTest.GetResponseMessage(systemEngineResponse, HttpStatusCode.OK)) // get system engine URL
                .ReturnsAsync(FireboltClientTest.GetResponseMessage("{\"query\":{\"query_id\": \"1\"},\"meta\":[{\"type\": \"int\", \"name\": \"1\"}],\"data\":[[\"1\"]]}", HttpStatusCode.OK)); // select 1 - to get infra version

            // Add additional responses
            foreach (var response in additionalResponses)
            {
                setupSequence.ReturnsAsync(response);
            }

            connection.CleanupCache();
            client.CleanupCache();
            connection.Open();

            return (connection, httpClientMock, client);
        }

        [TestCase("RUNNING", true)]
        [TestCase("ENDED_SUCCESSFULLY", false)]
        [TestCase("FAILED", false)]
        [TestCase("CANCELLED", false)]
        public void IsServerSideAsyncQueryRunning_WithStatus_ReturnsExpectedResult(string status, bool expected)
        {
            // Ensure the status in JSON matches exactly what the IsServerSideAsyncQueryRunning method expects
            string queryStatusJson = $"{{\"query\":{{\"query_id\":\"123\"}},\"meta\":[{{\"type\":\"string\",\"name\":\"status\"}},{{\"type\":\"string\",\"name\":\"query_id\"}}],\"data\":[[\"{status}\",\"456\"]]}}";
            var (connection, _, _) = SetupFireboltConnection(
                FireboltClientTest.GetResponseMessage(queryStatusJson, HttpStatusCode.OK)
            );

            bool result = connection.IsServerSideAsyncQueryRunning("test-token");

            That(result, Is.EqualTo(expected));
        }

        [TestCase("RUNNING", true)]
        [TestCase("ENDED_SUCCESSFULLY", false)]
        [TestCase("FAILED", false)]
        [TestCase("CANCELLED", false)]
        public async Task IsServerSideAsyncQueryRunningAsync_WithStatus_ReturnsExpectedResult(string status, bool expected)
        {
            string queryStatusJson = $"{{\"query\":{{\"query_id\":\"123\"}},\"meta\":[{{\"type\":\"string\",\"name\":\"status\"}},{{\"type\":\"string\",\"name\":\"query_id\"}}],\"data\":[[\"{status}\",\"456\"]]}}";
            var (connection, _, _) = SetupFireboltConnection(
                FireboltClientTest.GetResponseMessage(queryStatusJson, HttpStatusCode.OK)
            );

            bool result = await connection.IsServerSideAsyncQueryRunningAsync("test-token");

            That(result, Is.EqualTo(expected));
        }

        [TestCase("RUNNING", null)]
        [TestCase("ENDED_SUCCESSFULLY", true)]
        [TestCase("FAILED", false)]
        [TestCase("CANCELLED", false)]
        public void IsServerSideAsyncQuerySuccessful_WithStatus_ReturnsExpectedResult(string status, bool? expected)
        {
            string queryStatusJson = $"{{\"query\":{{\"query_id\":\"123\"}},\"meta\":[{{\"type\":\"string\",\"name\":\"status\"}},{{\"type\":\"string\",\"name\":\"query_id\"}}],\"data\":[[\"{status}\",\"456\"]]}}";
            var (connection, _, _) = SetupFireboltConnection(
                FireboltClientTest.GetResponseMessage(queryStatusJson, HttpStatusCode.OK)
            );

            bool? result = connection.IsServerSideAsyncQuerySuccessful("test-token");

            That(result, Is.EqualTo(expected));
        }

        [TestCase("RUNNING", null)]
        [TestCase("ENDED_SUCCESSFULLY", true)]
        [TestCase("FAILED", false)]
        [TestCase("CANCELLED", false)]
        public async Task IsServerSideAsyncQuerySuccessfulAsync_WithStatus_ReturnsExpectedResult(string status, bool? expected)
        {
            string queryStatusJson = $"{{\"query\":{{\"query_id\":\"123\"}},\"meta\":[{{\"type\":\"string\",\"name\":\"status\"}},{{\"type\":\"string\",\"name\":\"query_id\"}}],\"data\":[[\"{status}\",\"456\"]]}}";
            var (connection, _, _) = SetupFireboltConnection(
                FireboltClientTest.GetResponseMessage(queryStatusJson, HttpStatusCode.OK)
            );

            bool? result = await connection.IsServerSideAsyncQuerySuccessfulAsync("test-token");

            That(result, Is.EqualTo(expected));
        }

        [Test]
        public void CancelServerSideAsyncQuery_ValidToken_ReturnsTrue()
        {
            string queryStatusJson = "{\"query\":{\"query_id\":\"123\"},\"meta\":[{\"type\":\"string\",\"name\":\"status\"},{\"type\":\"string\",\"name\":\"query_id\"}],\"data\":[[\"RUNNING\",\"456\"]]}";
            string cancelResponseJson = "{\"query\":{\"query_id\":\"124\"},\"meta\":[],\"data\":[[]]}";
            var (connection, _, _) = SetupFireboltConnection(
                FireboltClientTest.GetResponseMessage(queryStatusJson, HttpStatusCode.OK),
                FireboltClientTest.GetResponseMessage(cancelResponseJson, HttpStatusCode.OK)
            );

            bool result = connection.CancelServerSideAsyncQuery("test-token");

            That(result, Is.True);
        }

        [Test]
        public async Task CancelServerSideAsyncQueryAsync_ValidToken_ReturnsTrue()
        {
            string queryStatusJson = "{\"query\":{\"query_id\":\"123\"},\"meta\":[{\"type\":\"string\",\"name\":\"status\"},{\"type\":\"string\",\"name\":\"query_id\"}],\"data\":[[\"RUNNING\",\"456\"]]}";
            string cancelResponseJson = "{\"query\":{\"query_id\":\"124\"},\"meta\":[],\"data\":[[]]}";
            var (connection, _, _) = SetupFireboltConnection(
                FireboltClientTest.GetResponseMessage(queryStatusJson, HttpStatusCode.OK),
                FireboltClientTest.GetResponseMessage(cancelResponseJson, HttpStatusCode.OK)
            );

            bool result = await connection.CancelServerSideAsyncQueryAsync("test-token");

            That(result, Is.True);
        }

        [Test]
        public void GetAsyncQueryStatus_EmptyToken_ThrowsArgumentNullException()
        {
            var (connection, _, _) = SetupFireboltConnection();
            var ex = Throws<ArgumentNullException>(() => connection.IsServerSideAsyncQueryRunning(""));
            That(ex, Is.Not.Null);
            That(ex!.ParamName, Is.EqualTo("token"));
        }

        [Test]
        public void CancelServerSideAsyncQuery_EmptyToken_ThrowsArgumentNullException()
        {
            var (connection, _, _) = SetupFireboltConnection();
            var ex = Throws<ArgumentNullException>(() => connection.CancelServerSideAsyncQuery(""));
            That(ex, Is.Not.Null);
            That(ex!.ParamName, Is.EqualTo("token"));
        }
    }
}