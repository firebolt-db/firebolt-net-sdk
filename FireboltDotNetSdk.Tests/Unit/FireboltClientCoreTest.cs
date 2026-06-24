using System.Net;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using Moq;
using Moq.Protected;
using static FireboltDotNetSdk.Tests.Helpers.HttpResponseHelper;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    public class FireboltClientCoreTest
    {
        [Test]
        public void DiscoveryConnectionStringSupportsSnakeCaseParametersWithoutCredentials()
        {
            var connection = new FireboltConnection(
                "engine_endpoint=localhost:3473;ssl_mode=none;database=db;engine_name=eng");

            Assert.Multiple(() =>
            {
                Assert.That(connection.Url, Is.EqualTo("localhost:3473"));
                Assert.That(connection.SslMode, Is.EqualTo("none"));
                Assert.That(connection.Database, Is.EqualTo("db"));
                Assert.That(connection.EngineName, Is.EqualTo("eng"));
                Assert.That(connection.Principal, Is.Empty);
                Assert.That(connection.Secret, Is.Empty);
            });
        }

        [TestCase("url=http://localhost:3473;engine_endpoint=http://other:3473",
            "Configuration error: either Url or EngineEndpoint must be provided but not both")]
        [TestCase("url=http://localhost:3473;ssl_mode=invalid",
            "Configuration error: ssl_mode must be either 'strict' or 'none'")]
        [TestCase("url=http://localhost:3473;username=user",
            "Configuration error: credentials must include both principal and secret")]
        [TestCase("url=http://localhost:3473;clientid=id;password=secret",
            "Configuration error: credential parameters must be provided as UserName/Password or ClientId/ClientSecret")]
        [TestCase("url=http://localhost:3473;username=user;clientid=id;password=secret",
            "Configuration error: either UserName or ClientId must be provided but not both")]
        [TestCase("url=http://localhost:3473;username=user;password=secret;clientsecret=secret",
            "Configuration error: either Password or ClientSecret must be provided but not both")]
        public void DiscoveryConnectionStringValidation(string connectionString, string expectedError)
        {
            var exception = Assert.Throws<FireboltException>(() => new FireboltConnection(connectionString));
            Assert.That(exception?.Message, Is.EqualTo(expectedError));
        }

        [Test]
        public void DiscoveryConnectionStringSupportsCredentialsWhenProvidedAsPair()
        {
            var connection = new FireboltConnection(
                "url=http://localhost:3473;client_id=id;client_secret=secret;account_name=acc");

            Assert.Multiple(() =>
            {
                Assert.That(connection.Principal, Is.EqualTo("id"));
                Assert.That(connection.Secret, Is.EqualTo("secret"));
                Assert.That(connection.Account, Is.EqualTo("acc"));
            });
        }

        [Test]
        public async Task OpenUsesDiscoveryEndpointAndSendsConnectionParametersOnQueries()
        {
            var (handlerMock, httpClient) = FireboltClientTest.GetHttpMocks();
            var connection = new FireboltConnection(
                "url=http://localhost:3473?timezone=UTC;database=db;engine=eng;ssl_mode=none");
            var client = new FireboltClientCore(connection, "http://localhost:3473?timezone=UTC", "none", httpClient);
            connection.Client = client;

            HttpRequestMessage? queryRequest = null;
            handlerMock.Protected()
                .SetupSequence<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(GetResponseMessage(
                    "{\"engine_url\":\"http://localhost:3473/query?tenant=local\",\"parameters\":{\"region\":\"dev\"}}",
                    HttpStatusCode.OK))
                .ReturnsAsync(() => GetResponseMessage("ok", HttpStatusCode.OK));

            handlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.Is<HttpRequestMessage>(request => request.Method == HttpMethod.Post),
                    ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((request, _) => queryRequest = request)
                .ReturnsAsync(GetResponseMessage("ok", HttpStatusCode.OK));

            await connection.OpenAsync(CancellationToken.None);
            var result = await client.ExecuteQueryAsync<string>(connection.EngineUrl, connection.Database, null,
                "SELECT 1", new HashSet<string>(), CancellationToken.None);

            Assert.Multiple(() =>
            {
                Assert.That(result, Is.EqualTo("ok"));
                Assert.That(connection.EngineUrl, Is.EqualTo("http://localhost:3473/query"));
                Assert.That(queryRequest, Is.Not.Null);
                Assert.That(queryRequest!.Headers.Contains("Authorization"), Is.False);
                Assert.That(queryRequest.RequestUri!.Query, Does.Contain("output_format=JSON_Compact"));
                Assert.That(queryRequest.RequestUri.Query, Does.Contain("database=db"));
                Assert.That(queryRequest.RequestUri.Query, Does.Contain("engine=eng"));
                Assert.That(queryRequest.RequestUri.Query, Does.Contain("timezone=UTC"));
                Assert.That(queryRequest.RequestUri.Query, Does.Contain("tenant=local"));
                Assert.That(queryRequest.RequestUri.Query, Does.Contain("region=dev"));
            });
        }

        [Test]
        public async Task OpenUsesFirstDiscoveredEngineEntry()
        {
            var (handlerMock, httpClient) = FireboltClientTest.GetHttpMocks();
            var connection = new FireboltConnection("url=http://localhost:3473;ssl_mode=none");
            var client = new FireboltClientCore(connection, "http://localhost:3473", "none", httpClient);
            connection.Client = client;

            HttpRequestMessage? queryRequest = null;
            handlerMock.Protected()
                .SetupSequence<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(GetResponseMessage(
                    "{\"engines\":[{\"endpoint\":\"http://node:3473/sql?node=1\"}],\"queryParameters\":{\"warehouse\":\"core\"}}",
                    HttpStatusCode.OK))
                .ReturnsAsync(GetResponseMessage("ok", HttpStatusCode.OK));

            handlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.Is<HttpRequestMessage>(request => request.Method == HttpMethod.Post),
                    ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((request, _) => queryRequest = request)
                .ReturnsAsync(GetResponseMessage("ok", HttpStatusCode.OK));

            await connection.OpenAsync(CancellationToken.None);
            await client.ExecuteQueryAsync<string>(connection.EngineUrl, connection.Database, null, "SELECT 1",
                new HashSet<string>(), CancellationToken.None);

            Assert.Multiple(() =>
            {
                Assert.That(connection.EngineUrl, Is.EqualTo("http://node:3473/sql"));
                Assert.That(queryRequest!.RequestUri!.Query, Does.Contain("node=1"));
                Assert.That(queryRequest.RequestUri.Query, Does.Contain("warehouse=core"));
            });
        }

        [Test]
        public async Task OpenFallsBackToConfiguredUrlWhenDiscoveryIsUnavailable()
        {
            var (handlerMock, httpClient) = FireboltClientTest.GetHttpMocks();
            var connection = new FireboltConnection("url=localhost:3473;ssl_mode=none");
            var client = new FireboltClientCore(connection, "localhost:3473", "none", httpClient);
            connection.Client = client;

            handlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(GetResponseMessage(HttpStatusCode.NotFound));

            await connection.OpenAsync(CancellationToken.None);

            Assert.That(connection.EngineUrl, Is.EqualTo("http://localhost:3473/"));
        }

        [TestCase("not-json")]
        [TestCase("{\"parameters\":{\"region\":\"dev\"}}")]
        public async Task OpenFallsBackToConfiguredUrlWhenDiscoveryBodyCannotResolveEndpoint(string discoveryBody)
        {
            var (handlerMock, httpClient) = FireboltClientTest.GetHttpMocks();
            var connection = new FireboltConnection("url=http://localhost:3473?timezone=UTC;ssl_mode=none");
            var client = new FireboltClientCore(connection, "http://localhost:3473?timezone=UTC", "none", httpClient);
            connection.Client = client;

            handlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(GetResponseMessage(discoveryBody, HttpStatusCode.OK));

            await connection.OpenAsync(CancellationToken.None);

            Assert.That(connection.EngineUrl, Is.EqualTo("http://localhost:3473/"));
        }

        [Test]
        public async Task OpenUsesHttpsForSchemalessUrlWhenSslModeIsStrict()
        {
            var (handlerMock, httpClient) = FireboltClientTest.GetHttpMocks();
            var connection = new FireboltConnection("url=localhost:3473");
            var client = new FireboltClientCore(connection, "localhost:3473", "strict", httpClient);
            connection.Client = client;

            HttpRequestMessage? discoveryRequest = null;
            handlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((request, _) => discoveryRequest = request)
                .ReturnsAsync(GetResponseMessage(HttpStatusCode.NotFound));

            await connection.OpenAsync(CancellationToken.None);

            Assert.Multiple(() =>
            {
                Assert.That(discoveryRequest!.RequestUri!.ToString(), Is.EqualTo("https://localhost:3473/.well-known/firebolt"));
                Assert.That(connection.EngineUrl, Is.EqualTo("https://localhost:3473/"));
            });
        }

        [Test]
        public async Task OpenReusesResolvedEndpointUntilCacheIsCleaned()
        {
            var (handlerMock, httpClient) = FireboltClientTest.GetHttpMocks();
            var connection = new FireboltConnection("url=http://localhost:3473;ssl_mode=none");
            var client = new FireboltClientCore(connection, "http://localhost:3473", "none", httpClient);
            connection.Client = client;

            handlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.Is<HttpRequestMessage>(request => request.Method == HttpMethod.Get),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(() => GetResponseMessage("{\"engineUrl\":\"http://localhost:3473/sql\"}", HttpStatusCode.OK));

            await connection.OpenAsync(CancellationToken.None);
            await connection.OpenAsync(CancellationToken.None);
            client.CleanupCache();
            await connection.OpenAsync(CancellationToken.None);

            handlerMock.Protected().Verify(
                "SendAsync",
                Times.Exactly(2),
                ItExpr.Is<HttpRequestMessage>(request => request.Method == HttpMethod.Get),
                ItExpr.IsAny<CancellationToken>());
        }

        [Test]
        public async Task CoreAuthMethodsAreNoOps()
        {
            var (_, httpClient) = FireboltClientTest.GetHttpMocks();
            var connection = new FireboltConnection("url=http://localhost:3473;ssl_mode=none");
            var client = new FireboltClientCore(connection, "http://localhost:3473", "none", httpClient);
            var token = await client.EstablishConnection();
            var account = await client.GetAccountIdByNameAsync("account", CancellationToken.None);

            Assert.Multiple(() =>
            {
                Assert.That(token, Is.Empty);
                Assert.That(account.id, Is.Null);
                Assert.That(account.infraVersion, Is.EqualTo(2));
            });
        }
    }
}
