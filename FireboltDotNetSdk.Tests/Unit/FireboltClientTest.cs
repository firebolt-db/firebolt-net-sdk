using System.Net;
using System.Text;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
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
        const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=test_pwd;account=accountname;endpoint=api.mock.firebolt.io";

        [Test]
        public void ExecuteQueryAsyncNullDataTest()
        {
            var handlerMock = new Mock<HttpMessageHandler>(MockBehavior.Strict);
            var httpClient = new HttpClient(handlerMock.Object);

            var connection = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient1(connection, Guid.NewGuid().ToString(), "any", "test.api.firebolt.io", null, "account", httpClient);
            var exception = Assert.ThrowsAsync<FireboltException>(() => client.ExecuteQueryAsync<string>("", "databaseName", null, "commandText", new HashSet<string>(), CancellationToken.None));

            Assert.That(exception, Is.Not.Null);
            Assert.That(exception!.Message, Is.EqualTo("Some parameters are null or empty: engineEndpoint:  or query: commandText"));
        }

        [Test]
        public void ExecuteQueryExceptionTest()
        {
            var handlerMock = new Mock<HttpMessageHandler>(MockBehavior.Strict);
            var httpClient = new HttpClient(handlerMock.Object);

            var connection = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient1(connection, "any", "any", "test.api.firebolt.io", null, "account", httpClient);
            var tokenField = typeof(FireboltClient).GetField("_token", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.That(tokenField, Is.Not.Null);
            tokenField!.SetValue(client, "abc");

            handlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Throws<HttpRequestException>();
            Assert.ThrowsAsync<HttpRequestException>(() => client.ExecuteQuery("DBName", "EngineURL", "null", "Select 1"));
        }

        [Test]
        public async Task ExecuteQueryTest()
        {
            var handlerMock = new Mock<HttpMessageHandler>(MockBehavior.Strict);
            var httpClient = new HttpClient(handlerMock.Object);

            var connection = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient1(connection, Guid.NewGuid().ToString(), "password", "http://test.api.firebolt.io", null, "account", httpClient);
            var tokenField = typeof(FireboltClient).GetField("_token", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
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
            var handlerMock = new Mock<HttpMessageHandler>(MockBehavior.Strict);
            var httpClient = new HttpClient(handlerMock.Object);

            var connection = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient1(connection, Guid.NewGuid().ToString(), "password", "http://test.api.firebolt-new-test.io", null, "account", httpClient);
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
            Assert.That(await client.ExecuteQueryAsync<string>("endpoint_url", "DBName", "account", "Select 1", new HashSet<string>(),
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
            var handlerMock = new Mock<HttpMessageHandler>(MockBehavior.Strict);
            var httpClient = new HttpClient(handlerMock.Object);

            var connection = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient1(connection, Guid.NewGuid().ToString(), "password", "http://test.api.firebolt-new-test.io", null, "account", httpClient);
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
            Assert.That(await client.ExecuteQueryAsync<string>("endpoint_url", "DBName", "account", "Select 1", new HashSet<string>(),
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
            var handlerMock = new Mock<HttpMessageHandler>(MockBehavior.Strict);
            var httpClient = new HttpClient(handlerMock.Object);

            var connection = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient1(connection, Guid.NewGuid().ToString(), "password", "http://test.api.firebolt-new-test.io", null, "account", httpClient);
            LoginResponse loginResponse = new("access_token", "3600", "Bearer");

            const string jsonErrorMessage = "{\"errors\":[{\"code\":\"400\",\"name\":\"Bad Request\",\"severity\":\"Error\",\"source\":\"Firebolt\",\"description\":\"Invalid query\",\"resolution\":\"Check the query and try again\",\"helpLink\":\"https://firebolt.io/docs\"}]}";

            handlerMock.Protected()
                .SetupSequence<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(GetResponseMessage(loginResponse, HttpStatusCode.OK))
                .ReturnsAsync(GetResponseMessage(jsonErrorMessage, HttpStatusCode.OK)); // despite the error, the response is OK
            var exception = Assert.ThrowsAsync<FireboltStructuredException>(() => client.ExecuteQueryAsync<string>("endpoint_url", "DBName", "account", "SELECT 1", new HashSet<string>(), CancellationToken.None));
            Assert.That(exception!.Message, Does.Contain("Error: Bad Request (400) - Firebolt, Invalid query, resolution: Check the query and try again"));
        }

        [Test]
        public void ExecuteQueryWithRetryWhenUnauthorizedExceptionTest()
        {
            var handlerMock = new Mock<HttpMessageHandler>(MockBehavior.Strict);
            var httpClient = new HttpClient(handlerMock.Object);

            var connection = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient1(connection, Guid.NewGuid().ToString(), "password", "http://test.api.firebolt-new-test.io", null, "account", httpClient);
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
            var exception = Assert.ThrowsAsync<FireboltException>(() => client.ExecuteQueryAsync<string>("endpoint_url", "DBName", "account", "SELECT 1", new HashSet<string>(), CancellationToken.None));
            Assert.That(exception!.Message, Does.Contain("The operation is unauthorized"));
        }

        [TestCase(connectionString, true)]
        [TestCase(connectionString + ";TokenStorage=None", false)]
        [TestCase(connectionString + ";TokenStorage=Memory", true)]
        [TestCase(connectionString + ";TokenStorage=File", true)]
        public async Task SuccessfulLoginWithCachedToken(string cs, bool cache)
        {
            // if token is cached the second connection will use the same token and the token is retrived only onece; otherwise the token is retrieved 2 times. 
            var handlerMock = new Mock<HttpMessageHandler>(MockBehavior.Strict);
            var httpClient = new HttpClient(handlerMock.Object);

            var connection = new FireboltConnection(cs);
            FireboltClient client = new FireboltClient1(connection, Guid.NewGuid().ToString(), "password", "http://test.api.firebolt-new-test.io", null, "account", httpClient);
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
            var handlerMock = new Mock<HttpMessageHandler>(MockBehavior.Strict);
            var httpClient = new HttpClient(handlerMock.Object);

            var connection = new FireboltConnection(connectionString);
            var client = new FireboltClient1(connection, Guid.NewGuid().ToString(), "password", "http://test.api.firebolt-new-test.io", null, "account", httpClient);
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
            var handlerMock = new Mock<HttpMessageHandler>(MockBehavior.Strict);
            var httpClient = new HttpClient(handlerMock.Object);

            var connection = new FireboltConnection(connectionString);
            FireboltClient client = new FireboltClient1(connection, Guid.NewGuid().ToString(), "password", "http://test.api.firebolt-new-test.io", null, "account", httpClient);
            const string errorMessage = "login failed";

            handlerMock.Protected().Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(GetResponseMessage(errorMessage, HttpStatusCode.Unauthorized));

            var actualErrorMessage = Assert.ThrowsAsync<FireboltException>(() => client.EstablishConnection())?.Message ?? "";
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
            var handlerMock = new Mock<HttpMessageHandler>(MockBehavior.Strict);
            var httpClient = new HttpClient(handlerMock.Object);

            var loginResponse = new LoginResponse("access_token", "3600", "Bearer");
            handlerMock.Protected().SetupSequence<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(GetResponseMessage(loginResponse, HttpStatusCode.OK))
                .ReturnsAsync(GetResponseMessage(new GetSystemEngineUrlResponse() { engineUrl = engineUrl },
                    HttpStatusCode.OK));

            var connection = new FireboltConnection("database=testdb.ib;clientid=testuser;clientsecret=test_pwd;account=accountname");
            var client = new FireboltClient2(connection, Guid.NewGuid().ToString(), "password", "", "test", "account", httpClient);
            var response = await client.GetSystemEngineUrl("my_account");
            Assert.That(response.engineUrl, Is.EqualTo(engineUrl));

            handlerMock.Protected().Verify(
                "SendAsync",
                Times.Exactly(2),
                ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>());
        }


        internal static HttpResponseMessage GetResponseMessage(object responseObject, HttpStatusCode httpStatusCode)
        {
            HttpResponseMessage response = GetResponseMessage(httpStatusCode);
            if (responseObject is string responseAsString)
            {
                response.Content = new StringContent(responseAsString);
            }
            else
            {
                response.Content = new StringContent(SerializeObject(responseObject), Encoding.UTF8, "application/json");
            }
            return response;
        }

        private static HttpResponseMessage GetResponseMessage(HttpStatusCode httpStatusCode)
        {
            return new HttpResponseMessage() { StatusCode = httpStatusCode };
        }
    }
}
