using System.Net;
using System.Text;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using Moq;
using static Newtonsoft.Json.JsonConvert;
using static FireboltDotNetSdk.Client.FireResponse;
using Windows.Media.ContentRestrictions;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    public class FireboltClientTest
    {

        [Test]
        public void ExecuteQueryAsyncNullDataTest()
        {
            Mock<HttpClient> httpClientMock = new();
            FireboltClient client = new FireboltClient(Guid.NewGuid().ToString(), "any", "test.api.firebolt.io", null, httpClientMock.Object);
            FireboltException? exception = Assert.ThrowsAsync<FireboltException>(() => client.ExecuteQueryAsync("", "databaseName", null, "commandText", new HashSet<string>(), CancellationToken.None));

            Assert.NotNull(exception);
            Assert.That(exception!.Message, Is.EqualTo("Some parameters are null or empty: engineEndpoint:  or query: commandText"));
        }

        [Test]
        public void ExecuteQueryExceptionTest()
        {
            Mock<HttpClient> httpClientMock = new();
            FireboltClient client = new FireboltClient("any", "any", "test.api.firebolt.io", null, httpClientMock.Object);
            var tokenField = client.GetType().GetField("_token", System.Reflection.BindingFlags.NonPublic
                                                                      | System.Reflection.BindingFlags.Instance);
            Assert.NotNull(tokenField);
            tokenField!.SetValue(client, "abc");
            httpClientMock.Setup(c => c.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>())).Throws<HttpRequestException>();
            HttpRequestException e = Assert.ThrowsAsync<HttpRequestException>(() => client.ExecuteQuery("DBName", "EngineURL", "null", "Select 1"));
        }

        [Test]
        public async Task ExecuteQueryTest()
        {
            Mock<HttpClient> httpClientMock = new();
            FireboltClient client = new FireboltClient(Guid.NewGuid().ToString(), "password", "http://test.api.firebolt.io", null, httpClientMock.Object);
            var tokenField = client.GetType().GetField("_token", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            Assert.NotNull(tokenField);

            tokenField!.SetValue(client, "abc");
            HttpResponseMessage okHttpResponse = GetResponseMessage("1", HttpStatusCode.OK);
            httpClientMock.Setup(c => c.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>())).ReturnsAsync(okHttpResponse);
            Assert.That(await client.ExecuteQuery("endpoint_url", "DBName", null, "Select 1"), Is.EqualTo("1"));
        }

        [Test]
        public async Task ExecuteQueryWithoutAccessTokenTest()
        {
            Mock<HttpClient> httpClientMock = new Mock<HttpClient>();
            FireboltClient client = new FireboltClient(Guid.NewGuid().ToString(), "password", "http://test.api.firebolt-new-test.io", null, httpClientMock.Object);
            FireResponse.LoginResponse loginResponse = new FireResponse.LoginResponse("access_token", "3600", "Bearer");

            //We expect 2 calls :
            //1. To establish a connection (fetch token).
            //2. Execute query
            httpClientMock.SetupSequence(p => p.SendAsync(It.IsAny<HttpRequestMessage>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(GetResponseMessage(loginResponse, HttpStatusCode.OK))
                .ReturnsAsync(GetResponseMessage("1", HttpStatusCode.OK));
            Assert.That(await client.ExecuteQueryAsync("endpoint_url", "DBName", "account", "Select 1", new HashSet<string>(),
                CancellationToken.None), Is.EqualTo("1"));

            httpClientMock.Verify(m => m.SendAsync(It.IsAny<HttpRequestMessage>(),
                It.IsAny<CancellationToken>()), Times.Exactly(2));
        }

        [Test]
        public async Task ExecuteQueryWithRetryWhenUnauthorizedTest()
        {
            Mock<HttpClient> httpClientMock = new Mock<HttpClient>();
            FireboltClient client = new FireboltClient(Guid.NewGuid().ToString(), "password", "http://test.api.firebolt-new-test.io", null, httpClientMock.Object);
            FireResponse.LoginResponse loginResponse = new FireResponse.LoginResponse("access_token", "3600", "Bearer");

            //We expect 4 calls :
            //1. to establish a connection (fetch token). Then a call to get a response to the query - which will return a 401.
            //2. Execute query - this call will return Unauthorized
            //3. Fetch a new token
            //4. Retry the query that previously triggered a 401
            httpClientMock.SetupSequence(p => p.SendAsync(It.IsAny<HttpRequestMessage>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(GetResponseMessage(loginResponse, HttpStatusCode.OK))
                .ReturnsAsync(GetResponseMessage(HttpStatusCode.Unauthorized))
                .ReturnsAsync(GetResponseMessage(loginResponse, HttpStatusCode.OK))
                .ReturnsAsync(GetResponseMessage("1", HttpStatusCode.OK));
            Assert.That(await client.ExecuteQueryAsync("endpoint_url", "DBName", "account", "Select 1", new HashSet<string>(),
                CancellationToken.None), Is.EqualTo("1"));

            httpClientMock.Verify(m => m.SendAsync(It.IsAny<HttpRequestMessage>(),
                 It.IsAny<CancellationToken>()), Times.Exactly(4));
        }

        [Test]
        public void ExecuteQueryWithRetryWhenUnauthorizedExceptionTest()
        {
            Mock<HttpClient> httpClientMock = new Mock<HttpClient>();
            FireboltClient client = new FireboltClient(Guid.NewGuid().ToString(), "password", "http://test.api.firebolt-new-test.io", null, httpClientMock.Object);
            FireResponse.LoginResponse loginResponse = new FireResponse.LoginResponse("access_token", "3600", "Bearer");


            //We expect 4 calls in total:
            //1. to establish a connection (fetch token). Then a call to get a response to the query - which will return a 401.
            //2. Execute query - this call will return Unauthorized
            //3. Fetch a new token
            //4. Retry the query again
            httpClientMock.SetupSequence(p => p.SendAsync(It.IsAny<HttpRequestMessage>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(GetResponseMessage(loginResponse, HttpStatusCode.OK))
                .ReturnsAsync(GetResponseMessage(HttpStatusCode.Unauthorized))
                .ReturnsAsync(GetResponseMessage(loginResponse, HttpStatusCode.OK))
                .ReturnsAsync(GetResponseMessage(HttpStatusCode.Unauthorized));
            var exception = Assert.ThrowsAsync<FireboltException>(() => client.ExecuteQueryAsync("endpoint_url", "DBName", "account", "SELECT 1", new HashSet<string>(), CancellationToken.None));
            Assert.IsTrue(exception!.Message.Contains("The operation is unauthorized"));
        }

        [Test]
        public async Task SuccessfulLoginWithCachedToken()
        {
            Mock<HttpClient> httpClientMock = new Mock<HttpClient>();
            FireboltClient client = new FireboltClient(Guid.NewGuid().ToString(), "password", "http://test.api.firebolt-new-test.io", null, httpClientMock.Object);
            FireResponse.LoginResponse loginResponse = new FireResponse.LoginResponse("access_token", "3600", "Bearer");
            httpClientMock.SetupSequence(p => p.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>())).ReturnsAsync(GetResponseMessage(loginResponse, HttpStatusCode.OK));
            string token1 = await client.EstablishConnection();
            Assert.That(token1, Is.EqualTo("access_token"));
            string token2 = await client.EstablishConnection(); // next time the token is taken from cache
            Assert.That(token2, Is.EqualTo("access_token"));
            httpClientMock.Verify(m => m.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()), Times.Exactly(1));
        }

        [Test]
        public async Task SuccessfulLoginWhenOldTokenIsExpired()
        {
            Mock<HttpClient> httpClientMock = new Mock<HttpClient>();
            FireboltClient client = new FireboltClient(Guid.NewGuid().ToString(), "password", "http://test.api.firebolt-new-test.io", null, httpClientMock.Object);
            FireResponse.LoginResponse loginResponse1 = new FireResponse.LoginResponse("access_token1", "0", "Bearer");
            FireResponse.LoginResponse loginResponse2 = new FireResponse.LoginResponse("access_token2", "3600", "Bearer");
            httpClientMock.SetupSequence(p => p.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(GetResponseMessage(loginResponse1, HttpStatusCode.OK))
            .ReturnsAsync(GetResponseMessage(loginResponse2, HttpStatusCode.OK));
            string token1 = await client.EstablishConnection();
            Assert.That(token1, Is.EqualTo("access_token1"));
            await Task.Delay(new TimeSpan(0, 0, 2)); // wait 1 seccond; the first token should be expired within 1 millisecond
            string token2 = await client.EstablishConnection(); // retrieve access token again because the first one is expired
            Assert.That(token2, Is.EqualTo("access_token2"));
            httpClientMock.Verify(m => m.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
        }

        [Test]
        public void NotAuthorizedLogin()
        {
            Mock<HttpClient> httpClientMock = new Mock<HttpClient>();
            FireboltClient client = new FireboltClient(Guid.NewGuid().ToString(), "password", "http://test.api.firebolt-new-test.io", null, httpClientMock.Object);
            string errorMessage = "login failed";
            httpClientMock.SetupSequence(p => p.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>())).ReturnsAsync(GetResponseMessage(errorMessage, HttpStatusCode.Unauthorized));
            string actualErrorMessage = Assert.ThrowsAsync<FireboltException>(() => client.EstablishConnection()).Message;
            Assert.IsTrue(actualErrorMessage.Contains("The operation is unauthorized"));
            Assert.IsTrue(actualErrorMessage.Contains(errorMessage));
            httpClientMock.Verify(m => m.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()), Times.Exactly(1));
        }

        [Test]
        public async Task GetSystemEngineUrl()
        {
            string engineUrl = "http://api.test.firebolt.io";
            Mock<HttpClient> httpClientMock = new Mock<HttpClient>();
            LoginResponse loginResponse = new LoginResponse("access_token", "3600", "Bearer");
            httpClientMock.SetupSequence(p => p.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(GetResponseMessage(loginResponse, HttpStatusCode.OK))
            .ReturnsAsync(GetResponseMessage(new GetSystemEngineUrlResponse() { engineUrl = engineUrl }, HttpStatusCode.OK));
            FireboltClient client = new FireboltClient(Guid.NewGuid().ToString(), "password", "", "test", httpClientMock.Object);
            GetSystemEngineUrlResponse response = await client.GetSystemEngineUrl("my_account");
            Assert.That(response.engineUrl, Is.EqualTo(engineUrl));
            httpClientMock.Verify(m => m.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
        }

        [Test]
        public async Task GetAccountIdByNameAsync()
        {
            Mock<HttpClient> httpClientMock = new Mock<HttpClient>();
            LoginResponse loginResponse = new LoginResponse("access_token", "3600", "Bearer");
            httpClientMock.SetupSequence(p => p.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(GetResponseMessage(loginResponse, HttpStatusCode.OK))
            .ReturnsAsync(GetResponseMessage(new GetAccountIdByNameResponse() { id = "my_account_id" }, HttpStatusCode.OK));
            FireboltClient client = new FireboltClient(Guid.NewGuid().ToString(), "password", "", "test", httpClientMock.Object);
            GetAccountIdByNameResponse response = await client.GetAccountIdByNameAsync("my_account_name", CancellationToken.None);
            Assert.That(response.id, Is.EqualTo("my_account_id"));
            httpClientMock.Verify(m => m.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
        }

        internal static HttpResponseMessage GetResponseMessage(Object responseObject, HttpStatusCode httpStatusCode)
        {
            HttpResponseMessage response = GetResponseMessage(httpStatusCode);
            if (responseObject is string)
            {
                response.Content = new StringContent((string)responseObject);
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
