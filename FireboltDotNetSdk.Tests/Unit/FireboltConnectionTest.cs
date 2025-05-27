using System.Data;
using System.Data.Common;
using System.Net;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;
using Moq;
using Moq.Protected;
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

            public MockFireboltConnection(string connectionString) : base(connectionString) { Client = new MockClient(""); }

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
            Assert.Multiple(() =>
            {
                Assert.That(cs.AccountId, Is.EqualTo("a123"));
                Assert.That(cs.InfraVersion, Is.EqualTo(1));
            });
        }

        [Test]
        public void ConnectionInfraVersion()
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=testpwd;account=accountname";
            var cs = new MockFireboltConnection(connectionString);
            FireboltConnection.CleanupCache();
            Assert.Multiple(() =>
            {
                Assert.That(cs.AccountId, Is.Null); // retrieving account ID initializes the InfraVersion that is 1 by default
                Assert.That(cs.InfraVersion, Is.EqualTo(1));
            });
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
            Assert.Multiple(() =>
            {
                Assert.That(cs.Database, Is.EqualTo("db1"));
                Assert.That(cs.DataSource, Is.EqualTo("db1"));
                Assert.That(cs.EngineName, Is.EqualTo("e1"));
            });
            cs.UpdateConnectionSettings(new FireboltConnectionStringBuilder("database=db2;username=usr;password=pwd;account=a2;engine=e2"), CancellationToken.None);
            Assert.Multiple(() =>
            {
                Assert.That(cs.Database, Is.EqualTo("db2"));
                Assert.That(cs.DataSource, Is.EqualTo("db2"));
                Assert.That(cs.EngineName, Is.EqualTo("e2"));
            });
        }

        [Test]
        public void ParsingNormalConnectionStringTest()
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=testpwd;account=accountname;endpoint=api.mock.firebolt.io";
            var cs = new FireboltConnection(connectionString);
            Assert.Multiple(() =>
            {
                Assert.That(cs.Endpoint, Is.EqualTo("api.mock.firebolt.io"));
                Assert.That(cs.Env, Is.EqualTo("mock"));
                Assert.That(cs.Database, Is.EqualTo("testdb.ib"));
                Assert.That(cs.DataSource, Is.EqualTo("testdb.ib"));
                Assert.That(cs.Account, Is.EqualTo("accountname"));
                Assert.That(cs.Secret, Is.EqualTo("testpwd"));
                Assert.That(cs.Principal, Is.EqualTo("testuser"));
            });
        }

        [Test]
        public void ParsingNoEndpointEnvConnectionStringTest()
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=testpwd;account=accountname;endpoint=some.weird.endpoint;env=mock";
            var cs = new FireboltConnection(connectionString);
            Assert.Multiple(() =>
            {
                Assert.That(cs.Endpoint, Is.EqualTo("some.weird.endpoint"));
                Assert.That(cs.Env, Is.EqualTo("mock"));
                Assert.That(cs.Database, Is.EqualTo("testdb.ib"));
                Assert.That(cs.Account, Is.EqualTo("accountname"));
                Assert.That(cs.Secret, Is.EqualTo("testpwd"));
                Assert.That(cs.Principal, Is.EqualTo("testuser"));
            });
        }

        [Test]
        public void ParsingEngineConnectionStringTest()
        {
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname;engine=diesel";
            var cs = new FireboltConnection(connectionString);
            Assert.Multiple(() =>
            {
                Assert.That(cs.Endpoint, Is.EqualTo(Constant.DEFAULT_ENDPOINT));
                Assert.That(cs.Env, Is.EqualTo(Constant.DEFAULT_ENV));
                Assert.That(cs.Account, Is.EqualTo("accountname"));
                Assert.That(cs.Secret, Is.EqualTo("testpwd"));
                Assert.That(cs.Principal, Is.EqualTo("testuser"));
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

        private static void ParsingWrongConnectionStringTestV2(string connectionString, string expectedErrorMessage)
        {
            var ex = Assert.Throws<FireboltException>(delegate { new FireboltConnection(connectionString); });
            Assert.That(ex?.Message, Is.EqualTo(expectedErrorMessage));
        }

        [TestCase("database=testdb.ib;username=testuser@domain.com;password=testpwd;account=;endpoint=api.mock.firebolt.io")]
        [TestCase("database=testdb.ib;username=testuser@domain.com;password=testpwd;endpoint=api.mock.firebolt.io")]
        public void ParsingMissAccountConnectionStringTestV1(string connectionString)
        {
            Assert.That(new FireboltConnection(connectionString), Is.Not.Null); // V1 does not require account and should succeed
            // IsNotNull is always true here and needed just to satisfy Sonar.
        }

        [Test]
        public void ParsingInvalidConnectionStringTest()
        {
            const string connectionString = "database=testdb.ib;clientid=test_user;clientsecret=test_pwd;account=account_name;endpoint=api.mock.firebolt.io";
            var cs = new FireboltConnection(connectionString);
            Assert.Multiple(() =>
            {
                Assert.That(cs.Endpoint, Is.EqualTo("api.mock.firebolt.io"));
                Assert.That(cs.Database, Is.EqualTo("testdb.ib"));
                Assert.That(cs.Account, Is.EqualTo("account_name"));
                Assert.That(cs.Secret, Is.EqualTo("test_pwd"));
                Assert.That(cs.Principal, Is.EqualTo("test_user"));
            });
        }

        [Test]
        public void ParsingIncompatibleEndpointAndEnvTest()
        {
            const string connectionString = "database=testdb.ib;clientid=test_user;clientsecret=test_pwd;account=account_name;endpoint=api.mock.firebolt.io;env=mock2";
            var ex = Assert.Throws<FireboltException>(
                    delegate { new FireboltConnection(connectionString); });
            Assert.That(ex?.Message, Is.EqualTo("Configuration error: environment mock2 and endpoint api.mock.firebolt.io are incompatible"));
        }

        [Test]
        public void OnSessionEstablishedTest()
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=testpwd;account=accountname;endpoint=api.mock.firebolt.io";
            var cs = new FireboltConnection(connectionString);
            cs.OnSessionEstablished();
            Assert.That(cs.State, Is.EqualTo(ConnectionState.Open));
        }

        [Test]
        public void CloseTest()
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=test_pwd;account=accountname;endpoint=api.mock.firebolt.io";
            Mock<HttpClient> httpClientMock = new();
            var cs = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient2(cs, Guid.NewGuid().ToString(), "any", "test.api.firebolt.io", null, "account", httpClientMock.Object);
            cs.Client = client;
            Assert.That(client, Is.SameAs(cs.Client));
            cs.Close();
            Assert.Multiple(() =>
            {
                Assert.That(cs.State, Is.EqualTo(ConnectionState.Closed));
                Assert.That(cs.Client, Is.EqualTo(null));
            });
        }

        [TestCase("test")]
        public void ParsingDatabaseHostnames(string hostname)
        {
            var ConnectionString = $"database={hostname}:test.ib;clientid=user;clientid=testuser;clientsecret=password;account=accountname;endpoint=api.mock.firebolt.io";
            var cs = new FireboltConnection(ConnectionString);
            Assert.That(cs.Database, Is.EqualTo("test:test.ib"));
        }

        [Test]
        public void CreateCommandTest()
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=passwordtest;account=accountname;endpoint=api.mock.firebolt.io;";
            var cs = new FireboltConnection(connectionString);
            var command = cs.CreateCommand();
            Assert.That(command, Is.Not.Null);
            Assert.That(command.Connection, Is.Not.Null);
            Assert.That(command.Connection.Database, Is.EqualTo("testdb.ib"));
        }

        [TestCase("Select 1")]
        public void CreateCommandTextTest(string commandText)
        {
            const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=passwordtest;account=accountname;endpoint=api.mock.firebolt.io";
            var cs = new FireboltConnection(connectionString);
            var command = cs.CreateCommand();
            command.CommandText = commandText;
            Assert.That(command.CommandText, Is.EqualTo("Select 1"));
        }

        [Test]
        public void CreateConnectionWithNullConnectionString()
        {
            var cs = new FireboltConnection("clientid=testuser;clientsecret=testpwd;account=accountname;engine=my");
            Assert.Throws<ArgumentNullException>(() => cs.ConnectionString = null);
        }

        [Test]
        public void SetSameConnectionString()
        {
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname;engine=my";
            var cs = new MockFireboltConnection(connectionString);
            Assert.That(cs.OpenedWithConnectionString, Is.EqualTo(null));
            cs.Open();
            Assert.Multiple(() =>
            {
                Assert.That(cs.OpenedWithConnectionString, Is.EqualTo(connectionString));
                Assert.That(cs.ClosedWithConnectionString, Is.EqualTo(null));
            });
            cs.Reset();

            cs.ConnectionString = connectionString;
            Assert.Multiple(() =>
            {
                Assert.That(cs.ClosedWithConnectionString, Is.EqualTo(null));
                Assert.That(cs.OpenedWithConnectionString, Is.EqualTo(null));
            });
            cs.Close();
            Assert.That(cs.ClosedWithConnectionString, Is.EqualTo(connectionString));
        }

        [Test]
        public void SetDifferentConnectionString()
        {
            const string connectionString1 = "clientid=testuser;clientsecret=testpwd;account=account1;engine=diesel";
            var cs = new MockFireboltConnection(connectionString1);
            Assert.That(cs.OpenedWithConnectionString, Is.EqualTo(null));
            cs.Open();
            Assert.Multiple(() =>
            {
                Assert.That(cs.OpenedWithConnectionString, Is.EqualTo(connectionString1));
                Assert.That(cs.ClosedWithConnectionString, Is.EqualTo(null));
            });
            const string connectionString2 = "clientid=testuser;clientsecret=testpwd;account=account2;engine=benzene";

            cs.ConnectionString = connectionString2;
            Assert.Multiple(() =>
            {
                Assert.That(cs.ClosedWithConnectionString, Is.EqualTo(connectionString1));
                Assert.That(cs.OpenedWithConnectionString, Is.EqualTo(connectionString2));
                Assert.That(cs.Account, Is.EqualTo("account2"));
            });
        }

        [Test]
        public void SetDifferentConnectionStringWithSameParameters()
        {
            const string connectionString1 = "clientid=testuser;clientsecret=testpwd;account=account1;engine=diesel;database=db;env=test";
            var cs = new MockFireboltConnection(connectionString1);
            Assert.That(cs.OpenedWithConnectionString, Is.EqualTo(null));
            cs.Open();
            Assert.Multiple(() =>
            {
                Assert.That(cs.OpenedWithConnectionString, Is.EqualTo(connectionString1));
                Assert.That(cs.ClosedWithConnectionString, Is.EqualTo(null));
            });
            const string connectionString2 = "account=account1;engine=diesel;database=db;env=test;clientid=testuser;clientsecret=testpwd";
            cs.ConnectionString = connectionString2;
            Assert.Multiple(() =>
            {
                // The connection was not re-opened
                Assert.That(cs.ClosedWithConnectionString, Is.EqualTo(null));
                Assert.That(cs.OpenedWithConnectionString, Is.EqualTo(connectionString1));
            });
        }

        [Test]
        public void SetSameDatabase()
        {
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname;database=db";
            var cs = new MockFireboltConnection(connectionString);
            Assert.That(cs.OpenedWithConnectionString, Is.EqualTo(null));
            cs.Open();
            Assert.Multiple(() =>
            {
                Assert.That(cs.OpenedWithConnectionString, Is.EqualTo(connectionString));
                Assert.That(cs.ClosedWithConnectionString, Is.EqualTo(null));
            });
            cs.Reset();

            cs.ChangeDatabase("db");
            Assert.Multiple(() =>
            {
                Assert.That(cs.ClosedWithConnectionString, Is.EqualTo(null));
                Assert.That(cs.OpenedWithConnectionString, Is.EqualTo(null));
            });
            cs.Close();
            Assert.That(cs.ClosedWithConnectionString, Is.EqualTo(connectionString));
        }

        [Test]
        public void SetDifferentDatabase()
        {
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname;database=one";
            var cs = new MockFireboltConnection(connectionString);
            Assert.That(cs.OpenedWithConnectionString, Is.EqualTo(null));
            cs.Open();
            Assert.Multiple(() =>
            {
                Assert.That(cs.OpenedWithConnectionString, Is.EqualTo(connectionString));
                Assert.That(cs.ClosedWithConnectionString, Is.EqualTo(null));
            });
            cs.Reset();

            cs.ChangeDatabase("two");
            string connectionString2 = "clientid=testuser;clientsecret=testpwd;account=accountname;database=two";
            Assert.Multiple(() =>
            {
                Assert.That(cs.ClosedWithConnectionString, Is.EqualTo(connectionString));
                Assert.That(cs.OpenedWithConnectionString, Is.EqualTo(connectionString2));
            });
            cs.Close();
            Assert.That(cs.ClosedWithConnectionString, Is.EqualTo(connectionString2));
        }

        [Test]
        public async Task SetSameDatabaseAsync()
        {
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname;database=db";
            var cs = new MockFireboltConnection(connectionString);
            Assert.That(cs.OpenedWithConnectionString, Is.EqualTo(null));
            cs.Open();
            Assert.Multiple(() =>
            {
                Assert.That(cs.OpenedWithConnectionString, Is.EqualTo(connectionString));
                Assert.That(cs.ClosedWithConnectionString, Is.EqualTo(null));
            });
            cs.Reset();

            await cs.ChangeDatabaseAsync("db");
            Assert.Multiple(() =>
            {
                Assert.That(cs.ClosedWithConnectionString, Is.EqualTo(null));
                Assert.That(cs.OpenedWithConnectionString, Is.EqualTo(null));
            });
            cs.Close();
            Assert.That(cs.ClosedWithConnectionString, Is.EqualTo(connectionString));
        }

        [Test]
        public void SetDifferentDatabaseAsync()
        {
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname;database=one";
            var cs = new MockFireboltConnection(connectionString);
            Assert.That(cs.OpenedWithConnectionString, Is.EqualTo(null));
            cs.Open();
            Assert.Multiple(() =>
            {
                Assert.That(cs.OpenedWithConnectionString, Is.EqualTo(connectionString));
                Assert.That(cs.ClosedWithConnectionString, Is.EqualTo(null));
            });
            cs.Reset();

            cs.ChangeDatabaseAsync("two");
            string connectionString2 = "clientid=testuser;clientsecret=testpwd;account=accountname;database=two";
            Assert.Multiple(() =>
            {
                Assert.That(cs.ClosedWithConnectionString, Is.EqualTo(connectionString));
                Assert.That(cs.OpenedWithConnectionString, Is.EqualTo(connectionString2));
            });
            cs.Close();
            Assert.That(cs.ClosedWithConnectionString, Is.EqualTo(connectionString2));
        }

        [Test]
        public void FakeTransaction()
        {
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname;engine=diesel";
            var cs = new FireboltConnection(connectionString);
            DbTransaction transaction = cs.BeginTransaction();
            Assert.That(transaction, Is.Not.Null);
            Assert.Multiple(() =>
            {
                Assert.That(transaction.SupportsSavepoints, Is.False);
                Assert.That(transaction.Connection, Is.SameAs(cs));
                Assert.That(transaction.IsolationLevel, Is.EqualTo(IsolationLevel.Unspecified));
            });
            transaction.Commit();
            Assert.Throws<NotImplementedException>(() => transaction.Rollback());
        }

        [Test]
        public void NotImplementedMethods()
        {
            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname;engine=diesel";
            var cs = new FireboltConnection(connectionString);
            Assert.Throws<NotSupportedException>(() => cs.EnlistTransaction(null));
            Assert.Throws<NotSupportedException>(() => cs.EnlistTransaction(new Mock<System.Transactions.Transaction>().Object));
            Assert.Throws<NotImplementedException>(() => cs.GetSchema());
            Assert.Throws<NotImplementedException>(() => cs.GetSchema("collection"));
            Assert.Throws<NotImplementedException>(() => cs.GetSchema("collection", Array.Empty<string>()));
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
            Assert.Throws<HttpRequestException>(() => cs.Open());
            // Restore client since connection.Close() is called on error
            cs.Client = client;
            Assert.ThrowsAsync<HttpRequestException>(async () => await cs.OpenAsync());
        }

        [Test]
        public void SuccessfulLogin()
        {
            var handlerMock = new Mock<HttpMessageHandler>(MockBehavior.Strict);
            var httpClient = new HttpClient(handlerMock.Object);

            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname";
            var cs = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient2(cs, Guid.NewGuid().ToString(), "password", "", "test", "account", httpClient);
            cs.Client = client;
            var loginResponse = new FireResponse.LoginResponse("access_token", "3600", "Bearer");
            var systemEngineResponse = new FireResponse.GetSystemEngineUrlResponse() { engineUrl = "api.test.firebolt.io" };

            handlerMock.Protected().SetupSequence<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(FireboltClientTest.GetResponseMessage(loginResponse, HttpStatusCode.OK)) // retrieve access token
            .ReturnsAsync(FireboltClientTest.GetResponseMessage(systemEngineResponse, HttpStatusCode.OK)) // get system engine URL
            .ReturnsAsync(FireboltClientTest.GetResponseMessage("{\"query\":{\"query_id\": \"1\"},\"meta\":[{\"type\": \"int\", \"name\": \"1\"}],\"data\":[[\"1\"]]}", HttpStatusCode.OK)) // select 1 - to get infra version
            ;
            FireboltConnection.CleanupCache();
            client.CleanupCache();
            cs.Open(); // should succeed
            // Due to Open does not return value the only way to validate that everything passed well is to validate that SendAsync was called exactly twice:
            // 1. to retrive token
            // 2. to retrieve system engine URL
            handlerMock.Protected().Verify(
                "SendAsync",
                Times.Exactly(3),
                ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>());
        }

        [Test]
        public void SuccessfulLoginTwice()
        {
            // Simulate the first connection. Calls of CleanupCache() guarnatee that the global caches are empty
            var handlerMock1 = new Mock<HttpMessageHandler>(MockBehavior.Strict);
            var httpClient1 = new HttpClient(handlerMock1.Object);

            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname";
            var cs1 = new FireboltConnection(connectionString);
            FireboltClient client1 = new FireboltClient2(cs1, Guid.NewGuid().ToString(), "password", "", "test", "account", httpClient1);
            FireboltConnection.CleanupCache();
            client1.CleanupCache();

            cs1.Client = client1;
            var loginResponse = new FireResponse.LoginResponse("access_token", "3600", "Bearer");
            var systemEngineResponse = new FireResponse.GetSystemEngineUrlResponse() { engineUrl = "api.test.firebolt.io" };

            handlerMock1.Protected().SetupSequence<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(FireboltClientTest.GetResponseMessage(loginResponse, HttpStatusCode.OK)) // retrieve access token
            .ReturnsAsync(FireboltClientTest.GetResponseMessage(systemEngineResponse, HttpStatusCode.OK)) // get system engine URL
            .ReturnsAsync(FireboltClientTest.GetResponseMessage("{\"query\":{\"query_id\": \"1\"},\"meta\":[{\"type\": \"int\", \"name\": \"1\"}],\"data\":[[\"1\"]]}", HttpStatusCode.OK)) // select 1 - to get infra version
            ;
            cs1.Open(); // should succeed
            // Due to Open does not return value the only way to validate that everything passed well is to validate that SendAsync was called exactly twice:
            // 1. to retrive token
            // 2. to retrieve system engine URL
            handlerMock1.Protected().Verify(
                "SendAsync",
                Times.Exactly(3),
                ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>());
            cs1.Close();

            // Now create the new connection. Due to caches are full we do not expect getting system engine URL and account ID. 
            var cs2 = new FireboltConnection(connectionString);
            var handlerMock2 = new Mock<HttpMessageHandler>(MockBehavior.Strict);
            var httpClient2 = new HttpClient(handlerMock2.Object);
            FireboltClient client2 = new FireboltClient2(cs2, Guid.NewGuid().ToString(), "password", "", "test", "account", httpClient2);
            cs2.Client = client2;

            handlerMock2.Protected().SetupSequence<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(FireboltClientTest.GetResponseMessage(loginResponse, HttpStatusCode.OK)) // retrieve access token
            .ReturnsAsync(FireboltClientTest.GetResponseMessage("{\"query\":{\"query_id\": \"1\"},\"meta\":[{\"type\": \"int\", \"name\": \"1\"}],\"data\":[[\"1\"]]}", HttpStatusCode.OK)) // select 1 - to get infra version
            ;
            cs2.Open(); // should succeed
            handlerMock2.Protected().Verify(
                "SendAsync",
                Times.Exactly(2),
                ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>());
        }


        [TestCase("Running", "api.firebolt.io", "", "api.firebolt.io")]
        [TestCase("ENGINE_STATE_RUNNING", "api.firebolt.io?account_id=01hf9pchg0mnrd2g3hypm1dea4&engine=max_test", "", "api.firebolt.io")]
        public void SuccessfulLoginWithEngineName(string engineStatus, string engineUrl, string catalogs, string expectedEngineUrl)
        {
            var handlerMock = new Mock<HttpMessageHandler>(MockBehavior.Strict);
            var httpClient = new HttpClient(handlerMock.Object);

            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname;engine=diesel";
            var cs = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient2(cs, Guid.NewGuid().ToString(), "password", "", "test", "account", httpClient);
            var loginResponse = new FireResponse.LoginResponse("access_token", "3600", "Bearer");
            var systemEngineResponse = new FireResponse.GetSystemEngineUrlResponse() { engineUrl = "api.test.firebolt.io" };

            handlerMock.Protected().SetupSequence<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(
                    FireboltClientTest.GetResponseMessage(loginResponse, HttpStatusCode.OK)) // retrieve access token
                .ReturnsAsync(
                    FireboltClientTest.GetResponseMessage(systemEngineResponse,
                        HttpStatusCode.OK)) // get system engine URL
                .ReturnsAsync(FireboltClientTest.GetResponseMessage(
                    "{\"query\":{\"query_id\": \"3\"},\"meta\":[],\"data\":[]}", HttpStatusCode.OK)) // USE ENGINE
                .ReturnsAsync(FireboltClientTest.GetResponseMessage(
                    "{\"query\":{\"query_id\": \"1\"},\"meta\":[{\"type\": \"string\"}, {\"name\": \"version()\"}],\"data\":[[\"1.2.3\"]]}",
                    HttpStatusCode.OK)); // get version

            cs.Client = client;
            FireboltConnection.CleanupCache();
            client.CleanupCache();
            cs.Open(); // should succeed
            Assert.That(cs.ServerVersion, Is.EqualTo("1.2.3"));
            handlerMock.Protected().Verify(
                "SendAsync",
                Times.Exactly(4),
                ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>());
        }

        /// <summary>
        /// Helper method to set up a FireboltConnection with mocked HttpClient for testing
        /// </summary>
        /// <param name="additionalResponses">Optional list of HttpResponseMessage objects to append after initial authentication</param>
        /// <returns>the FireboltConnection</returns>
        private static FireboltConnection SetupFireboltConnection(params HttpResponseMessage[] additionalResponses)
        {
            var handlerMock = new Mock<HttpMessageHandler>(MockBehavior.Strict);
            var httpClient = new HttpClient(handlerMock.Object);

            const string connectionString = "clientid=testuser;clientsecret=testpwd;account=accountname";
            var connection = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient2(connection, Guid.NewGuid().ToString(), "password", "", "test", "account", httpClient);
            connection.Client = client;

            var loginResponse = new FireResponse.LoginResponse("access_token", "3600", "Bearer");
            var systemEngineResponse = new FireResponse.GetSystemEngineUrlResponse() { engineUrl = "api.test.firebolt.io" };

            var setupSequence = handlerMock.Protected().SetupSequence<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(FireboltClientTest.GetResponseMessage(loginResponse, HttpStatusCode.OK)) // retrieve access token
                .ReturnsAsync(FireboltClientTest.GetResponseMessage(systemEngineResponse, HttpStatusCode.OK)) // get system engine URL
                .ReturnsAsync(FireboltClientTest.GetResponseMessage(
                    "{\"query\":{\"query_id\": \"1\"},\"meta\":[{\"type\": \"int\", \"name\": \"1\"}],\"data\":[[\"1\"]]}",
                    HttpStatusCode.OK)); // select 1 - to get infra version

            // Add additional responses
            foreach (var response in additionalResponses)
            {
                setupSequence.ReturnsAsync(response);
            }

            FireboltConnection.CleanupCache();
            client.CleanupCache();
            connection.Open();

            return connection;
        }

        [TestCase("RUNNING", true)]
        [TestCase("ENDED_SUCCESSFULLY", false)]
        [TestCase("FAILED", false)]
        [TestCase("CANCELLED", false)]
        public void IsServerSideAsyncQueryRunning_WithStatus_ReturnsExpectedResult(string status, bool expected)
        {
            // Ensure the status in JSON matches exactly what the IsServerSideAsyncQueryRunning method expects
            string queryStatusJson = $"{{\"query\":{{\"query_id\":\"123\"}},\"meta\":[{{\"type\":\"string\",\"name\":\"status\"}},{{\"type\":\"string\",\"name\":\"query_id\"}}],\"data\":[[\"{status}\",\"456\"]]}}";
            var connection = SetupFireboltConnection(
                FireboltClientTest.GetResponseMessage(queryStatusJson, HttpStatusCode.OK)
            );

            bool result = connection.IsServerSideAsyncQueryRunning("test-token");

            Assert.That(result, Is.EqualTo(expected));
        }

        [TestCase("RUNNING", true)]
        [TestCase("ENDED_SUCCESSFULLY", false)]
        [TestCase("FAILED", false)]
        [TestCase("CANCELLED", false)]
        public async Task IsServerSideAsyncQueryRunningAsync_WithStatus_ReturnsExpectedResult(string status, bool expected)
        {
            string queryStatusJson = $"{{\"query\":{{\"query_id\":\"123\"}},\"meta\":[{{\"type\":\"string\",\"name\":\"status\"}},{{\"type\":\"string\",\"name\":\"query_id\"}}],\"data\":[[\"{status}\",\"456\"]]}}";
            var connection = SetupFireboltConnection(
                FireboltClientTest.GetResponseMessage(queryStatusJson, HttpStatusCode.OK)
            );

            bool result = await connection.IsServerSideAsyncQueryRunningAsync("test-token");

            Assert.That(result, Is.EqualTo(expected));
        }

        [TestCase("RUNNING", null)]
        [TestCase("ENDED_SUCCESSFULLY", true)]
        [TestCase("FAILED", false)]
        [TestCase("CANCELLED", false)]
        public void IsServerSideAsyncQuerySuccessful_WithStatus_ReturnsExpectedResult(string status, bool? expected)
        {
            string queryStatusJson = $"{{\"query\":{{\"query_id\":\"123\"}},\"meta\":[{{\"type\":\"string\",\"name\":\"status\"}},{{\"type\":\"string\",\"name\":\"query_id\"}}],\"data\":[[\"{status}\",\"456\"]]}}";
            var connection = SetupFireboltConnection(
                FireboltClientTest.GetResponseMessage(queryStatusJson, HttpStatusCode.OK)
            );

            bool? result = connection.IsServerSideAsyncQuerySuccessful("test-token");

            Assert.That(result, Is.EqualTo(expected));
        }

        [TestCase("RUNNING", null)]
        [TestCase("ENDED_SUCCESSFULLY", true)]
        [TestCase("FAILED", false)]
        [TestCase("CANCELLED", false)]
        public async Task IsServerSideAsyncQuerySuccessfulAsync_WithStatus_ReturnsExpectedResult(string status, bool? expected)
        {
            string queryStatusJson = $"{{\"query\":{{\"query_id\":\"123\"}},\"meta\":[{{\"type\":\"string\",\"name\":\"status\"}},{{\"type\":\"string\",\"name\":\"query_id\"}}],\"data\":[[\"{status}\",\"456\"]]}}";
            var connection = SetupFireboltConnection(
                FireboltClientTest.GetResponseMessage(queryStatusJson, HttpStatusCode.OK)
            );

            bool? result = await connection.IsServerSideAsyncQuerySuccessfulAsync("test-token");

            Assert.That(result, Is.EqualTo(expected));
        }

        [Test]
        public void CancelServerSideAsyncQuery_ValidToken_ReturnsTrue()
        {
            string queryStatusJson = "{\"query\":{\"query_id\":\"123\"},\"meta\":[{\"type\":\"string\",\"name\":\"status\"},{\"type\":\"string\",\"name\":\"query_id\"}],\"data\":[[\"RUNNING\",\"456\"]]}";
            string cancelResponseJson = "{\"query\":{\"query_id\":\"124\"},\"meta\":[],\"data\":[[]]}";
            var connection = SetupFireboltConnection(
                FireboltClientTest.GetResponseMessage(queryStatusJson, HttpStatusCode.OK),
                FireboltClientTest.GetResponseMessage(cancelResponseJson, HttpStatusCode.OK)
            );

            bool result = connection.CancelServerSideAsyncQuery("test-token");

            Assert.That(result, Is.True);
        }

        [Test]
        public async Task CancelServerSideAsyncQueryAsync_ValidToken_ReturnsTrue()
        {
            string queryStatusJson = "{\"query\":{\"query_id\":\"123\"},\"meta\":[{\"type\":\"string\",\"name\":\"status\"},{\"type\":\"string\",\"name\":\"query_id\"}],\"data\":[[\"RUNNING\",\"456\"]]}";
            string cancelResponseJson = "{\"query\":{\"query_id\":\"124\"},\"meta\":[],\"data\":[[]]}";
            var connection = SetupFireboltConnection(
                FireboltClientTest.GetResponseMessage(queryStatusJson, HttpStatusCode.OK),
                FireboltClientTest.GetResponseMessage(cancelResponseJson, HttpStatusCode.OK)
            );

            bool result = await connection.CancelServerSideAsyncQueryAsync("test-token");

            Assert.That(result, Is.True);
        }

        [Test]
        public void GetAsyncQueryStatus_EmptyToken_ThrowsArgumentNullException()
        {
            var connection = SetupFireboltConnection();
            var ex = Assert.Throws<ArgumentNullException>(() => connection.IsServerSideAsyncQueryRunning(""));
            Assert.That(ex, Is.Not.Null);
            Assert.That(ex!.ParamName, Is.EqualTo("token"));
        }

        [Test]
        public void CancelServerSideAsyncQuery_EmptyToken_ThrowsArgumentNullException()
        {
            var connection = SetupFireboltConnection();
            var ex = Assert.Throws<ArgumentNullException>(() => connection.CancelServerSideAsyncQuery(""));
            Assert.That(ex, Is.Not.Null);
            Assert.That(ex!.ParamName, Is.EqualTo("token"));
        }
    }
}