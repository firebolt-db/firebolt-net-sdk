using System.Net;
using System.Text;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;
using Moq;
using static Newtonsoft.Json.JsonConvert;
using static FireboltDotNetSdk.Client.FireResponse;
using FireboltNETSDK.Exception;
using Moq.Protected;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    public class FireboltClientTest
    {
        const string connectionString =
            "database=testdb.ib;clientid=testuser;clientsecret=test_pwd;account=accountname;endpoint=api.mock.firebolt.io";

        [Test]
        public void ExecuteQueryAsyncNullDataTest()
        {
            var (_, httpClient) = GetHttpMocks();

            var connection = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient1(connection, Guid.NewGuid().ToString(), "any",
                "test.api.firebolt.io", null, "account", httpClient);
            var exception = Assert.ThrowsAsync<FireboltException>(() =>
                client.ExecuteQueryAsync<string>("", "databaseName", null, "commandText", new HashSet<string>(),
                    CancellationToken.None));

            Assert.That(exception, Is.Not.Null);
            Assert.That(exception!.Message,
                Is.EqualTo("Some parameters are null or empty: engineEndpoint:  or query: commandText"));
        }

        [Test]
        public void ExecuteQueryExceptionTest()
        {
            var (handlerMock, httpClient) = GetHttpMocks();

            var connection = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient1(connection, "any", "any", "test.api.firebolt.io", null,
                "account", httpClient);
            var tokenField = typeof(FireboltClient).GetField("_token",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.That(tokenField, Is.Not.Null);
            tokenField!.SetValue(client, "abc");

            handlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Throws<HttpRequestException>();
            Assert.ThrowsAsync<HttpRequestException>(() =>
                client.ExecuteQuery("DBName", "EngineURL", "null", "Select 1"));
        }

        [Test]
        public async Task ExecuteQueryTest()
        {
            var (handlerMock, httpClient) = GetHttpMocks();

            var connection = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient1(connection, Guid.NewGuid().ToString(), "password",
                "http://test.api.firebolt.io", null, "account", httpClient);
            var tokenField = typeof(FireboltClient).GetField("_token",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.That(tokenField, Is.Not.Null);
            tokenField!.SetValue(client, "abc");

            handlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(GetResponseMessage("1", HttpStatusCode.OK));
            Assert.That(await client.ExecuteQuery("endpoint_url", "DBName", null, "Select 1"), Is.EqualTo("1"));
        }

        [Test]
        public async Task ExecuteQueryWithoutAccessTokenTest()
        {
            var (handlerMock, httpClient) = GetHttpMocks();

            var connection = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient1(connection, Guid.NewGuid().ToString(), "password",
                "http://test.api.firebolt-new-test.io", null, "account", httpClient);
            var loginResponse = new LoginResponse("access_token", "3600", "Bearer");

            //We expect 2 calls :
            //1. To establish a connection (fetch token).
            //2. Execute query
            handlerMock.Protected()
                .SetupSequence<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(GetResponseMessage(loginResponse, HttpStatusCode.OK))
                .ReturnsAsync(GetResponseMessage("1", HttpStatusCode.OK));
            Assert.That(await client.ExecuteQueryAsync<string>("endpoint_url", "DBName", "account", "Select 1",
                new HashSet<string>(),
                CancellationToken.None), Is.EqualTo("1"));

            handlerMock.Protected().Verify(
                "SendAsync",
                Times.Exactly(2),
                ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>());
        }

        [Test]
        public async Task ExecuteQueryWithRetryWhenUnauthorizedTest()
        {
            var (handlerMock, httpClient) = GetHttpMocks();

            var connection = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient1(connection, Guid.NewGuid().ToString(), "password",
                "http://test.api.firebolt-new-test.io", null, "account", httpClient);
            var loginResponse = new LoginResponse("access_token", "3600", "Bearer");

            //We expect 4 calls :
            //1. to establish a connection (fetch token). Then a call to get a response to the query - which will return a 401.
            //2. Execute query - this call will return Unauthorized
            //3. Fetch a new token
            //4. Retry the query that previously triggered a 401
            handlerMock.Protected()
                .SetupSequence<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(GetResponseMessage(loginResponse, HttpStatusCode.OK))
                .ReturnsAsync(GetResponseMessage(HttpStatusCode.Unauthorized))
                .ReturnsAsync(GetResponseMessage(loginResponse, HttpStatusCode.OK))
                .ReturnsAsync(GetResponseMessage("1", HttpStatusCode.OK));
            Assert.That(await client.ExecuteQueryAsync<string>("endpoint_url", "DBName", "account", "Select 1",
                new HashSet<string>(),
                CancellationToken.None), Is.EqualTo("1"));

            handlerMock.Protected().Verify(
                "SendAsync",
                Times.Exactly(4),
                ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>());
        }


        [Test]
        public void ExecuteQueryWithJsonErrorReturned()
        {
            var (handlerMock, httpClient) = GetHttpMocks();

            var connection = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient1(connection, Guid.NewGuid().ToString(), "password",
                "http://test.api.firebolt-new-test.io", null, "account", httpClient);
            LoginResponse loginResponse = new("access_token", "3600", "Bearer");

            const string jsonErrorMessage =
                "{\"errors\":[{\"code\":\"400\",\"name\":\"Bad Request\",\"severity\":\"Error\",\"source\":\"Firebolt\",\"description\":\"Invalid query\",\"resolution\":\"Check the query and try again\",\"helpLink\":\"https://firebolt.io/docs\"}]}";

            handlerMock.Protected()
                .SetupSequence<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(GetResponseMessage(loginResponse, HttpStatusCode.OK))
                .ReturnsAsync(GetResponseMessage(jsonErrorMessage,
                    HttpStatusCode.OK)); // despite the error, the response is OK
            var exception = Assert.ThrowsAsync<FireboltStructuredException>(() =>
                client.ExecuteQueryAsync<string>("endpoint_url", "DBName", "account", "SELECT 1", new HashSet<string>(),
                    CancellationToken.None));
            Assert.That(exception!.Message,
                Does.Contain(
                    "Error: Bad Request (400) - Firebolt, Invalid query, resolution: Check the query and try again"));
        }

        [Test]
        public void ExecuteQueryWithRetryWhenUnauthorizedExceptionTest()
        {
            var (handlerMock, httpClient) = GetHttpMocks();

            var connection = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient1(connection, Guid.NewGuid().ToString(), "password",
                "http://test.api.firebolt-new-test.io", null, "account", httpClient);
            var loginResponse = new LoginResponse("access_token", "3600", "Bearer");


            //We expect 4 calls in total:
            //1. to establish a connection (fetch token). Then a call to get a response to the query - which will return a 401.
            //2. Execute query - this call will return Unauthorized
            //3. Fetch a new token
            //4. Retry the query again
            handlerMock.Protected()
                .SetupSequence<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(GetResponseMessage(loginResponse, HttpStatusCode.OK))
                .ReturnsAsync(GetResponseMessage(HttpStatusCode.Unauthorized))
                .ReturnsAsync(GetResponseMessage(loginResponse, HttpStatusCode.OK))
                .ReturnsAsync(GetResponseMessage(HttpStatusCode.Unauthorized));
            var exception = Assert.ThrowsAsync<FireboltException>(() => client.ExecuteQueryAsync<string>("endpoint_url",
                "DBName", "account", "SELECT 1", new HashSet<string>(), CancellationToken.None));
            Assert.That(exception!.Message, Does.Contain("The operation is unauthorized"));
        }

        [TestCase(connectionString, true)]
        [TestCase(connectionString + ";TokenStorage=None", false)]
        [TestCase(connectionString + ";TokenStorage=Memory", true)]
        [TestCase(connectionString + ";TokenStorage=File", true)]
        public async Task SuccessfulLoginWithCachedToken(string cs, bool cache)
        {
            // if token is cached the second connection will use the same token and the token is retrived only onece; otherwise the token is retrieved 2 times. 
            var (handlerMock, httpClient) = GetHttpMocks();

            var connection = new FireboltConnection(cs);
            FireboltClient client = new FireboltClient1(connection, Guid.NewGuid().ToString(), "password",
                "http://test.api.firebolt-new-test.io", null, "account", httpClient);
            var loginResponse1 = new LoginResponse("access_token1", "3600", "Bearer");
            var loginResponse2 = new LoginResponse("access_token2", "3600", "Bearer");

            handlerMock.Protected()
                .SetupSequence<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(GetResponseMessage(loginResponse1, HttpStatusCode.OK))
                .ReturnsAsync(GetResponseMessage(loginResponse2, HttpStatusCode.OK));
            var token1 = await client.EstablishConnection();
            Assert.That(token1, Is.EqualTo("access_token1"));
            var token2 = await client.EstablishConnection(); // next time the token is taken from cache
            Assert.That(token2, Is.EqualTo(cache ? "access_token1" : "access_token2"));

            handlerMock.Protected().Verify(
                "SendAsync",
                Times.Exactly(cache ? 1 : 2),
                ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>());
        }

        [Test]
        public async Task SuccessfulLoginWhenOldTokenIsExpired()
        {
            var (handlerMock, httpClient) = GetHttpMocks();

            var connection = new FireboltConnection(connectionString);
            var client = new FireboltClient1(connection, Guid.NewGuid().ToString(), "password",
                "http://test.api.firebolt-new-test.io", null, "account", httpClient);
            var loginResponse1 = new LoginResponse("access_token1", "0", "Bearer"); // expires immediately
            var loginResponse2 = new LoginResponse("access_token2", "3600", "Bearer");

            handlerMock.Protected()
                .SetupSequence<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(GetResponseMessage(loginResponse1, HttpStatusCode.OK))
                .ReturnsAsync(GetResponseMessage(loginResponse2, HttpStatusCode.OK));

            var token1 = await client.EstablishConnection();
            Assert.That(token1, Is.EqualTo("access_token1"));

            await Task.Delay(TimeSpan.FromSeconds(1)); // Wait for the first token to expire

            var token2 = await client.EstablishConnection(); // Should fetch new token
            Assert.That(token2, Is.EqualTo("access_token2"));

            handlerMock.Protected().Verify(
                "SendAsync",
                Times.Exactly(2),
                ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>());
        }

        [Test]
        public void NotAuthorizedLogin()
        {
            var (handlerMock, httpClient) = GetHttpMocks();

            var connection = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient1(connection, Guid.NewGuid().ToString(), "password",
                "http://test.api.firebolt-new-test.io", null, "account", httpClient);
            const string errorMessage = "login failed";

            handlerMock.Protected().Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(GetResponseMessage(errorMessage, HttpStatusCode.Unauthorized));

            var actualErrorMessage =
                Assert.ThrowsAsync<FireboltException>(() => client.EstablishConnection())?.Message ?? "";
            Assert.Multiple(() =>
            {
                Assert.That(actualErrorMessage, Does.Contain("The operation is unauthorized"));
                Assert.That(actualErrorMessage, Does.Contain(errorMessage));
            });

            handlerMock.Protected().Verify("SendAsync", Times.Exactly(1), ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>());
        }

        [Test]
        public async Task GetSystemEngineUrl()
        {
            const string engineUrl = "http://api.test.firebolt.io";
            var (handlerMock, httpClient) = GetHttpMocks();

            var loginResponse = new LoginResponse("access_token", "3600", "Bearer");
            handlerMock.Protected().SetupSequence<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(GetResponseMessage(loginResponse, HttpStatusCode.OK))
                .ReturnsAsync(GetResponseMessage(new GetSystemEngineUrlResponse() { engineUrl = engineUrl },
                    HttpStatusCode.OK));

            var connection =
                new FireboltConnection(
                    "database=testdb.ib;clientid=testuser;clientsecret=test_pwd;account=accountname");
            var client = new FireboltClient2(connection, Guid.NewGuid().ToString(), "password", "", "test", "account",
                httpClient);
            var response = await client.GetSystemEngineUrl("my_account");
            Assert.That(response.engineUrl, Is.EqualTo(engineUrl));

            handlerMock.Protected().Verify(
                "SendAsync",
                Times.Exactly(2),
                ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>());
        }

        internal static (Mock<HttpMessageHandler> handlerMock, HttpClient httpClient) GetHttpMocks()
        {
            var handlerMock = new Mock<HttpMessageHandler>(MockBehavior.Strict);
            var httpClient = new HttpClient(handlerMock.Object);
            return (handlerMock, httpClient);
        }


        internal static HttpResponseMessage GetResponseMessage(object responseObject, HttpStatusCode httpStatusCode,
            Dictionary<string, string>? headers = null)
        {
            HttpResponseMessage response = GetResponseMessage(httpStatusCode);
            if (responseObject is string responseAsString)
            {
                response.Content = new StringContent(responseAsString);
            }
            else
            {
                response.Content =
                    new StringContent(SerializeObject(responseObject), Encoding.UTF8, "application/json");
            }

            if (headers != null)
            {
                foreach (var header in headers)
                {
                    response.Headers.Add(header.Key, header.Value);
                }
            }

            return response;
        }

        private static HttpResponseMessage GetResponseMessage(HttpStatusCode httpStatusCode)
        {
            return new HttpResponseMessage() { StatusCode = httpStatusCode };
        }

        #region FireboltClient2 Caching Tests

        [Test]
        public void TestClient2_ConnectionId_IsUnique()
        {
            var (_, httpClient1) = GetHttpMocks();
            var (_, httpClient2) = GetHttpMocks();

            var connection1 = new FireboltConnection(connectionString);
            var connection2 = new FireboltConnection(connectionString);

            var client1 =
                new FireboltClient2(connection1, "id1", "secret1", "endpoint1", null, "account1", httpClient1);
            var client2 =
                new FireboltClient2(connection2, "id2", "secret2", "endpoint2", null, "account2", httpClient2);

            // Use reflection to access private _connectionId field
            var connectionIdField = typeof(FireboltClient2).GetField("_connectionId",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.That(connectionIdField, Is.Not.Null);

            var connectionId1 = connectionIdField!.GetValue(client1) as string;
            var connectionId2 = connectionIdField.GetValue(client2) as string;
            Assert.Multiple(() =>
            {
                Assert.That(connectionId1, Is.Not.Null);
                Assert.That(connectionId2, Is.Not.Null);
            });
            Assert.That(connectionId1, Is.Not.EqualTo(connectionId2), "Each client should have a unique connection ID");
        }

        [Test]
        public async Task TestClient2_EstablishConnection_CachesJwtToken()
        {
            CacheService.Instance.Clear();

            var (handlerMock, httpClient) = GetHttpMocks();

            var connection = new FireboltConnection(connectionString);
            var client = new FireboltClient2(connection, "test_id", "test_secret", "http://test.api.firebolt.io", null,
                "test_account", httpClient);

            var loginResponse = new LoginResponse("cached_token", "3600", "Bearer");

            // Mock login response
            handlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(GetResponseMessage(loginResponse, HttpStatusCode.OK));

            // First call should fetch token
            var token1 = await client.EstablishConnection();
            Assert.That(token1, Is.EqualTo("cached_token"));

            // Verify token is cached
            var cacheKey = new CacheKey("test_id", "test_secret", "test_account");
            var cache = CacheService.Instance.Get(cacheKey.GetValue());
            Assert.That(cache, Is.Not.Null);
            Assert.Multiple(() =>
            {
                Assert.That(cache!.GetCachedToken(), Is.Not.Null);
                Assert.That(cache.GetCachedToken()!.Access_token, Is.EqualTo("cached_token"));
            });

            // Second call should use cached token (no additional HTTP call)
            var token2 = await client.EstablishConnection();
            Assert.That(token2, Is.EqualTo("cached_token"));

            // Verify only one HTTP call was made (for login)
            handlerMock.Protected().Verify(
                "SendAsync",
                Times.Once(),
                ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>());

            CacheService.Instance.Clear();
        }

        [Test]
        public async Task TestClient2_EstablishConnection_ForceRefreshBypassesCache()
        {
            CacheService.Instance.Clear();
            TokenMemoryStorage.tokens.Clear();

            var (handlerMock, httpClient) = GetHttpMocks();

            var connection = new FireboltConnection(connectionString);
            var client = new FireboltClient2(connection, "test_id", "test_secret", "http://test.api.firebolt.io", null,
                "test_account", httpClient);

            var loginResponse1 = new LoginResponse("token1", "3600", "Bearer");
            var loginResponse2 = new LoginResponse("token2", "3600", "Bearer");

            // Mock login responses
            handlerMock.Protected()
                .SetupSequence<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(GetResponseMessage(loginResponse1, HttpStatusCode.OK))
                .ReturnsAsync(GetResponseMessage(loginResponse2, HttpStatusCode.OK));

            // First call
            var token1 = await client.EstablishConnection();
            Assert.That(token1, Is.EqualTo("token1"));

            // Force refresh should bypass cache and get new token
            var token2 = await client.EstablishConnection(forceTokenRefresh: true);
            Assert.That(token2, Is.EqualTo("token2"));

            // Verify two HTTP calls were made
            handlerMock.Protected().Verify(
                "SendAsync",
                Times.Exactly(2),
                ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>());

            CacheService.Instance.Clear();
            TokenMemoryStorage.tokens.Clear();
        }

        [Test]
        public async Task TestClient2_GetHttpRequest_AddsConnectionIdToUserAgent()
        {
            CacheService.Instance.Clear();

            var (handlerMock, httpClient) = GetHttpMocks();
            httpClient.DefaultRequestHeaders.Add("User-Agent", ".NETSDK/.NET6_1.0.0");

            var connection = new FireboltConnection(connectionString);
            var client = new FireboltClient2(connection, "test_id", "test_secret", "http://test.api.firebolt.io", null,
                "test_account", httpClient);

            // Set up token
            var tokenField = typeof(FireboltClient).GetField("_token",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.That(tokenField, Is.Not.Null);
            tokenField!.SetValue(client, "test_token");

            HttpRequestMessage? capturedRequest = null;

            handlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((req, token) => capturedRequest = req)
                .ReturnsAsync(GetResponseMessage("result", HttpStatusCode.OK));

            // Execute a query to trigger GetHttpRequest
            await client.ExecuteQuery("http://endpoint", "db", null, "SELECT 1");

            // Verify User-Agent header contains connection info
            Assert.That(capturedRequest, Is.Not.Null);
            var userAgentHeader = capturedRequest!.Headers.GetValues("User-Agent").FirstOrDefault();
            Assert.That(userAgentHeader, Is.Not.Null);
            Assert.That(userAgentHeader, Does.Contain("connId:"), "User-Agent should contain connection ID");

            CacheService.Instance.Clear();
        }

        [Test]
        public async Task TestClient2_GetHttpRequest_AddsCachedConnectionInfoToUserAgent()
        {
            CacheService.Instance.Clear();
            TokenMemoryStorage.tokens.Clear();

            var (handlerMock1, httpClient1) = GetHttpMocks();
            var (handlerMock2, httpClient2) = GetHttpMocks();

            httpClient1.DefaultRequestHeaders.Add("User-Agent", ".NETSDK/.NET6_1.0.0");
            httpClient2.DefaultRequestHeaders.Add("User-Agent", ".NETSDK/.NET6_1.0.0");

            var connection1 = new FireboltConnection(connectionString);
            var connection2 = new FireboltConnection(connectionString);

            var client1 = new FireboltClient2(connection1, "test_id", "test_secret", "http://test.api.firebolt.io",
                null, "test_account", httpClient1);
            var client2 = new FireboltClient2(connection2, "test_id", "test_secret", "http://test.api.firebolt.io",
                null, "test_account", httpClient2);

            var loginResponse = new LoginResponse("test_token", "3600", "Bearer");

            // Set up mock for client1 login
            handlerMock1.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(GetResponseMessage(loginResponse, HttpStatusCode.OK));

            // First client establishes connection and populates cache
            await client1.EstablishConnection();

            // Set up token for client2
            var tokenField = typeof(FireboltClient).GetField("_token",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.That(tokenField, Is.Not.Null);
            tokenField!.SetValue(client2, "test_token");

            HttpRequestMessage? capturedRequest = null;

            handlerMock2.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((req, token) => capturedRequest = req)
                .ReturnsAsync(GetResponseMessage("result", HttpStatusCode.OK));

            // Second client uses cached connection
            await client2.ExecuteQuery("http://endpoint", "db", null, "SELECT 1");

            // Verify User-Agent header contains both connId and cachedConnId
            Assert.That(capturedRequest, Is.Not.Null);
            var userAgentHeader = capturedRequest!.Headers.GetValues("User-Agent").FirstOrDefault();
            Assert.That(userAgentHeader, Is.Not.Null);
            Assert.That(userAgentHeader, Does.Contain("connId:"), "User-Agent should contain current connection ID");
            Assert.That(userAgentHeader, Does.Contain("cachedConnId:"),
                "User-Agent should contain cached connection ID");
            Assert.That(userAgentHeader, Does.Contain("-Memory"), "User-Agent should indicate cache source");

            CacheService.Instance.Clear();
            TokenMemoryStorage.tokens.Clear();
        }

        [Test]
        public void TestClient2_CacheIsolation_DifferentAccountsUseSeparateCaches()
        {
            CacheService.Instance.Clear();

            var (_, httpClient1) = GetHttpMocks();
            var (_, httpClient2) = GetHttpMocks();

            var connection1 = new FireboltConnection(connectionString);
            var connection2 = new FireboltConnection(connectionString);

            // Create clients with different accounts
            var client1 = new FireboltClient2(connection1, "test_id", "test_secret", "http://test.api.firebolt.io",
                null, "account1", httpClient1);
            var client2 = new FireboltClient2(connection2, "test_id", "test_secret", "http://test.api.firebolt.io",
                null, "account2", httpClient2);

            // Populate cache for account1
            var cacheKey1 = new CacheKey("test_id", "test_secret", "account1");
            var cache1 = CacheService.Instance.GetOrCreate(cacheKey1.GetValue(), "conn1");
            cache1.SetDatabaseValidated("db1");

            // Populate cache for account2
            var cacheKey2 = new CacheKey("test_id", "test_secret", "account2");
            var cache2 = CacheService.Instance.GetOrCreate(cacheKey2.GetValue(), "conn2");
            cache2.SetDatabaseValidated("db2");

            // Verify caches are isolated
            var retrieved1 = CacheService.Instance.Get(cacheKey1.GetValue());
            var retrieved2 = CacheService.Instance.Get(cacheKey2.GetValue());
            Assert.Multiple(() =>
            {
                Assert.That(retrieved1, Is.Not.Null);
                Assert.That(retrieved2, Is.Not.Null);
            });
            Assert.That(retrieved1, Is.Not.SameAs(retrieved2));
            Assert.Multiple(() =>
            {
                Assert.That(retrieved1!.IsDatabaseValidated("db1"), Is.True);
                Assert.That(retrieved1.IsDatabaseValidated("db2"), Is.False);
                Assert.That(retrieved2!.IsDatabaseValidated("db2"), Is.True);
                Assert.That(retrieved2.IsDatabaseValidated("db1"), Is.False);
            });
            CacheService.Instance.Clear();
        }

        [Test]
        public void TestClient2_CacheKey_IncludesAccountInHash()
        {
            var cacheKey1 = new CacheKey("client_id", "client_secret", "account1");
            var cacheKey2 = new CacheKey("client_id", "client_secret", "account2");

            // Same credentials but different account should produce different cache keys
            Assert.That(cacheKey1.GetValue(), Is.Not.EqualTo(cacheKey2.GetValue()));
        }

        [Test]
        public void TestClient2_CacheConnection_DefaultsToTrue()
        {
            // Connection string without cache_connection parameter should default to true
            const string connectionStringWithoutCache =
                "database=testdb;clientid=testuser;clientsecret=test_pwd;account=accountname;engine=test_engine";
            var connection = new FireboltConnection(connectionStringWithoutCache);

            Assert.That(connection.IsCacheConnectionEnabled, Is.True, "cache_connection should default to true");
        }

        [Test]
        public void TestClient2_CacheConnection_CanBeDisabled()
        {
            // Connection string with CacheConnection=false
            const string connectionStringWithCachingDisabled =
                "database=testdb;clientid=testuser;clientsecret=test_pwd;account=accountname;engine=test_engine;CacheConnection=false";
            var connection = new FireboltConnection(connectionStringWithCachingDisabled);

            Assert.That(connection.IsCacheConnectionEnabled, Is.False,
                "CacheConnection should be false when explicitly set");
        }

        [Test]
        public void TestClient2_CacheConnection_CanBeEnabled()
        {
            // Connection string with CacheConnection=true
            const string connectionStringWithCachingEnabled =
                "database=testdb;clientid=testuser;clientsecret=test_pwd;account=accountname;engine=test_engine;CacheConnection=true";
            var connection = new FireboltConnection(connectionStringWithCachingEnabled);

            Assert.That(connection.IsCacheConnectionEnabled, Is.True,
                "CacheConnection should be true when explicitly set");
        }

        [Test]
        public async Task TestClient2_EstablishConnection_DisabledCachingUsesBaseImplementation()
        {
            CacheService.Instance.Clear();
            TokenMemoryStorage.tokens.Clear();

            var (handlerMock, httpClient) = GetHttpMocks();

            const string connectionStringWithCachingDisabled =
                "database=testdb;clientid=test_id;clientsecret=test_secret;account=test_account;CacheConnection=false";
            var connection = new FireboltConnection(connectionStringWithCachingDisabled);
            var client = new FireboltClient2(connection, "test_id", "test_secret", "http://test.api.firebolt.io", null,
                "test_account", httpClient);

            var loginResponse1 = new LoginResponse("token1", "3600", "Bearer");
            var loginResponse2 = new LoginResponse("token2", "3600", "Bearer");

            // Mock login responses
            handlerMock.Protected()
                .SetupSequence<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(GetResponseMessage(loginResponse1, HttpStatusCode.OK))
                .ReturnsAsync(GetResponseMessage(loginResponse2, HttpStatusCode.OK));

            // First call
            var token1 = await client.EstablishConnection();
            Assert.That(token1, Is.EqualTo("token1"));

            // Second call - should fetch new token (not from ConnectionCache, but may use legacy TokenStorage)
            var token2 = await client.EstablishConnection();
            // Verify ConnectionCache was not populated
            var cacheKey = new CacheKey("test_id", "test_secret", "test_account");
            var cache = CacheService.Instance.Get(cacheKey.GetValue());
            Assert.Multiple(() =>
            {
                Assert.That(token2, Is.Not.Null);
                Assert.That(cache, Is.Null);
            });
            CacheService.Instance.Clear();
            TokenMemoryStorage.tokens.Clear();
        }

        [Test]
        public void TestClient2_CacheConnection_ConnectionStringBuilder_ParsesCorrectly()
        {
            var builder1 =
                new FireboltConnectionStringBuilder(
                    "clientid=test;clientsecret=secret;account=acc;CacheConnection=true");
            Assert.That(builder1.CacheConnection, Is.True, "Should parse CacheConnection=true");

            var builder2 =
                new FireboltConnectionStringBuilder(
                    "clientid=test;clientsecret=secret;account=acc;CacheConnection=false");
            Assert.That(builder2.CacheConnection, Is.False, "Should parse CacheConnection=false");

            var builder3 = new FireboltConnectionStringBuilder("clientid=test;clientsecret=secret;account=acc");
            Assert.That(builder3.CacheConnection, Is.True, "Should default to true when not specified");
        }

        [Test]
        public void TestClient2_CacheConnection_ConnectionSettings_StoresCorrectly()
        {
            var builder1 =
                new FireboltConnectionStringBuilder(
                    "clientid=test;clientsecret=secret;account=acc;CacheConnection=true");
            var settings1 = builder1.BuildSettings();
            Assert.That(settings1.CacheConnection, Is.True, "Settings should store CacheConnection=true");

            var builder2 =
                new FireboltConnectionStringBuilder(
                    "clientid=test;clientsecret=secret;account=acc;CacheConnection=false");
            var settings2 = builder2.BuildSettings();
            Assert.That(settings2.CacheConnection, Is.False, "Settings should store CacheConnection=false");

            var builder3 = new FireboltConnectionStringBuilder("clientid=test;clientsecret=secret;account=acc");
            var settings3 = builder3.BuildSettings();
            Assert.That(settings3.CacheConnection, Is.True, "Settings should default to true");
        }

        [Test]
        public async Task TestClient2_CacheConnection_DisabledDoesNotPopulateCache()
        {
            CacheService.Instance.Clear();
            TokenMemoryStorage.tokens.Clear();

            var (handlerMock, httpClient) = GetHttpMocks();
            const string connectionStringCachingDisabled =
                "database=testdb;clientid=test_id;clientsecret=test_secret;account=test_account;CacheConnection=false";
            var connection = new FireboltConnection(connectionStringCachingDisabled);
            var client = new FireboltClient2(connection, "test_id", "test_secret", "http://test.api.firebolt.io", null,
                "test_account", httpClient);

            var loginResponse = new LoginResponse("token1", "3600", "Bearer");
            handlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(GetResponseMessage(loginResponse, HttpStatusCode.OK));

            await client.EstablishConnection();

            // Verify ConnectionCache was not created
            var cacheKey = new CacheKey("test_id", "test_secret", "test_account");
            var cache = CacheService.Instance.Get(cacheKey.GetValue());

            Assert.That(cache, Is.Null, "ConnectionCache should not be created when caching is disabled");

            CacheService.Instance.Clear();
            TokenMemoryStorage.tokens.Clear();
        }

        [Test]
        public async Task TestClient2_CacheConnection_EnabledPopulatesCache()
        {
            CacheService.Instance.Clear();
            TokenMemoryStorage.tokens.Clear();

            var (handlerMock, httpClient) = GetHttpMocks();
            const string connectionStringCachingEnabled =
                "database=testdb;clientid=test_id;clientsecret=test_secret;account=test_account;CacheConnection=true";
            var connection = new FireboltConnection(connectionStringCachingEnabled);
            var client = new FireboltClient2(connection, "test_id", "test_secret", "http://test.api.firebolt.io", null,
                "test_account", httpClient);

            var loginResponse = new LoginResponse("token1", "3600", "Bearer");
            handlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(GetResponseMessage(loginResponse, HttpStatusCode.OK));

            await client.EstablishConnection();
            // Verify ConnectionCache was created
            var cacheKey = new CacheKey("test_id", "test_secret", "test_account");
            var cache = CacheService.Instance.Get(cacheKey.GetValue());

            Assert.That(cache, Is.Not.Null, "ConnectionCache should be created when caching is enabled");
            Assert.That(cache.GetCachedToken(), Is.Not.Null, "JWT token should be cached");

            CacheService.Instance.Clear();
            TokenMemoryStorage.tokens.Clear();
        }

        [Test]
        public void TestClient2_CacheConnection_CaseInsensitiveParameter()
        {
            // Test that the parameter name is case-insensitive
            var conn1 = new FireboltConnection("clientid=id;clientsecret=secret;account=acc;cacheconnection=false");
            var conn2 = new FireboltConnection("clientid=id;clientsecret=secret;account=acc;CACHECONNECTION=false");
            var conn3 = new FireboltConnection("clientid=id;clientsecret=secret;account=acc;CacheConnection=false");

            Assert.Multiple(() =>
            {
                Assert.That(conn1.IsCacheConnectionEnabled, Is.False, "Lowercase should work");
                Assert.That(conn2.IsCacheConnectionEnabled, Is.False, "Uppercase should work");
                Assert.That(conn3.IsCacheConnectionEnabled, Is.False, "PascalCase should work");
            });
        }

        [Test]
        public async Task TestClient2_GetAndSetDatabaseProperties_CachesDatabaseOnFirstConnect()
        {
            // Test that GetAndSetDatabaseProperties caches database validation on first connection
            CacheService.Instance.Clear();

            const string connectionString =
                "database=testdb;clientid=testuser;clientsecret=test_pwd;account=test_account;engine=my_engine;CacheConnection=true";

            var (handlerMock, httpClient) = GetHttpMocks();
            var connection = new FireboltConnection(connectionString);
            var client = new FireboltClient2(connection, "test_id", "test_secret", "", "test", "test_account",
                httpClient);
            connection.Client = client;

            var loginResponse = new LoginResponse("test_token", "3600", "Bearer");
            var systemEngineResponse = new GetSystemEngineUrlResponse
            {
                engineUrl = "https://system-engine.firebolt.io?account_id=acc123"
            };

            handlerMock.Protected()
                .SetupSequence<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(GetResponseMessage(loginResponse, HttpStatusCode.OK))
                .ReturnsAsync(GetResponseMessage(systemEngineResponse, HttpStatusCode.OK))
                .ReturnsAsync(GetResponseMessage("", HttpStatusCode.OK, new Dictionary<string, string>
                {
                    { "Firebolt-Update-Parameters", "database=testdb" }
                })) // USE DATABASE (GetAndSetDatabaseProperties)
                .ReturnsAsync(GetResponseMessage("", HttpStatusCode.OK, new Dictionary<string, string>
                {
                    { "Firebolt-Update-Endpoint", "https://my-engine.firebolt.io?engine=my_engine" }
                })); // USE ENGINE (GetAndSetEngineProperties)

            // First connection - should call GetAndSetDatabaseProperties and cache database validation
            await connection.OpenAsync();

            // Verify database and engine were cached
            var cacheKey = new CacheKey("test_id", "test_secret", "test_account");
            var cache = CacheService.Instance.Get(cacheKey.GetValue());

            Assert.That(cache, Is.Not.Null, "Cache should be created");
            // Verify database validation was cached by GetAndSetDatabaseProperties
            Assert.That(cache!.IsDatabaseValidated("testdb"), Is.True,
                "GetAndSetDatabaseProperties should have marked database 'testdb' as validated");
            // Verify engine options were cached by GetAndSetEngineProperties
            var cachedEngineOptions = cache.GetEngineOptions("my_engine");
            Assert.That(cachedEngineOptions, Is.Not.Null,
                "GetAndSetEngineProperties should have cached engine options");

            Assert.Multiple(() =>
            {
                //todo FIR-51994
                Assert.That(cachedEngineOptions!.EngineUrl, Is.EqualTo("https://system-engine.firebolt.io"),
                    "Cached engine URL should match the Firebolt-Update-Endpoint header value");

                var engineParam = cachedEngineOptions.Parameters.FirstOrDefault(p => p.Key == "engine");
                Assert.That(engineParam.Key, Is.EqualTo("engine"), "Engine parameter should exist");
                Assert.That(engineParam.Value, Is.EqualTo("my_engine"),
                    "Engine parameter value should be 'my_engine'");
            });
            await connection.CloseAsync();
        }

        #endregion
    }
}
