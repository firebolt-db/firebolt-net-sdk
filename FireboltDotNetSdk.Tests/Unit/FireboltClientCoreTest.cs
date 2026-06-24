using System.Net;
using FireboltDotNetSdk.Client;
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
    }
}
