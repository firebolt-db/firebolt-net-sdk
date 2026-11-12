using System.Net;
using System.Web;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Tests.Helpers;
using Moq;
using Moq.Protected;
using static FireboltDotNetSdk.Client.FireResponse;
using static FireboltDotNetSdk.Tests.Helpers.HttpResponseHelper;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    public class QueryUrlEncodingTest
    {
        private const string ConnectionString = "database=db;clientid=id;clientsecret=secret;endpoint=ep;account=a;env=mock";

        // Each value breaks an unencoded query string differently: '&' and '=' split it, '#' truncates it,
        // '+' decodes to a space and an existing '%XX' sequence decodes to another character.
        private static readonly string[] TrickyValues =
        {
            "a&b=c", "db#1", "my db+x", "100%25off", "100%", "x?y/z", "engïne 日本", "'quoted'", "plain_name",
        };

        private Mock<HttpMessageHandler> _handlerMock = null!;
        private HttpClient _httpClient = null!;
        private List<Uri> _sentUris = null!;

        [SetUp]
        public void SetUp()
        {
            (_handlerMock, _httpClient) = FireboltClientTest.GetHttpMocks();
            _sentUris = new List<Uri>();
            RespondWith(() => GetResponseMessage("{}", HttpStatusCode.OK));
        }

        private void RespondWith(Func<HttpResponseMessage> response)
        {
            _handlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
                .Callback((HttpRequestMessage request, CancellationToken _) => _sentUris.Add(request.RequestUri!))
                .ReturnsAsync(response);
        }

        private FireboltClient CreateClient(FireboltConnection connection)
        {
            var client = new FireboltClient1(connection, Guid.NewGuid().ToString(), "secret", "api.mock.firebolt.io", null, "a", _httpClient);
            client.SetToken("token");
            connection.Client = client;
            return client;
        }

        private Task ExecuteQuery(FireboltClient client, string? database, params string[] setParams)
        {
            return client.ExecuteQueryAsync<string>("engine.mock.firebolt.io", database, null, "SELECT 1",
                new HashSet<string>(setParams), CancellationToken.None);
        }

        // Decodes the way a server does, including '+' as a space, so missing and double encoding both fail.
        private static System.Collections.Specialized.NameValueCollection SentParameters(Uri uri)
        {
            return HttpUtility.ParseQueryString(uri.Query);
        }

        [TestCaseSource(nameof(TrickyValues))]
        public async Task DatabaseNameIsEncoded(string database)
        {
            var client = CreateClient(new FireboltConnection(ConnectionString));

            await ExecuteQuery(client, database);

            var parameters = SentParameters(_sentUris.Single());
            Assert.That(_sentUris.Single().Fragment, Is.Empty);
            Assert.That(parameters["database"], Is.EqualTo(database));
            Assert.That(parameters.AllKeys, Is.EquivalentTo(new[] { "output_format", "database" }));
        }

        [TestCaseSource(nameof(TrickyValues))]
        public async Task EngineNameIsEncoded(string engine)
        {
            var connection = new FireboltConnection(ConnectionString) { InfraVersion = 2 };
            connection.UpdateConnectionSettings(new FireboltConnectionStringBuilder(ConnectionString) { Engine = engine }, CancellationToken.None);
            var client = CreateClient(connection);

            await ExecuteQuery(client, "db");

            var parameters = SentParameters(_sentUris.Single());
            Assert.That(parameters["engine"], Is.EqualTo(engine));
            Assert.That(parameters.AllKeys, Is.EquivalentTo(new[] { "output_format", "database", "engine", "query_label" }));
        }

        [TestCaseSource(nameof(TrickyValues))]
        public async Task SetParameterValueIsEncoded(string value)
        {
            var client = CreateClient(new FireboltConnection(ConnectionString));

            await ExecuteQuery(client, "db", "time_zone=" + value);

            var parameters = SentParameters(_sentUris.Single());
            Assert.That(parameters["time_zone"], Is.EqualTo(value));
            Assert.That(parameters.AllKeys, Is.EquivalentTo(new[] { "output_format", "database", "time_zone" }));
        }

        [Test]
        public async Task SetParameterCannotInjectOtherParameters()
        {
            var client = CreateClient(new FireboltConnection(ConnectionString));

            await ExecuteQuery(client, "db", "query_label=x&database=other#frag");

            var parameters = SentParameters(_sentUris.Single());
            Assert.That(parameters["database"], Is.EqualTo("db"));
            Assert.That(parameters["query_label"], Is.EqualTo("x&database=other#frag"));
        }

        [Test]
        public async Task SetParameterWithoutValueIsSentAsBareName()
        {
            var client = CreateClient(new FireboltConnection(ConnectionString));

            await ExecuteQuery(client, "db", "flag");

            Assert.That(_sentUris.Single().Query, Does.EndWith("&flag"));
        }

        [TestCaseSource(nameof(TrickyValues))]
        public async Task ParameterFromServerHeaderIsEncoded(string value)
        {
            var connection = new FireboltConnection(ConnectionString);
            var client = CreateClient(connection);
            RespondWith(() => GetResponseMessage("{}", HttpStatusCode.OK,
                new Dictionary<string, string> { { "Firebolt-Update-Parameters", "query_label=" + value } }));

            await ExecuteQuery(client, "db");
            await ExecuteQuery(client, "db");

            Assert.That(SentParameters(_sentUris.Last())["query_label"], Is.EqualTo(value));
        }

        [TestCaseSource(nameof(TrickyValues))]
        public async Task ParameterFromUpdateEndpointHeaderIsDecodedAndEncodedAgain(string engine)
        {
            var connection = new FireboltConnection(ConnectionString) { InfraVersion = 2 };
            var client = CreateClient(connection);
            var endpoint = $"https://engine.mock.firebolt.io?engine={Uri.EscapeDataString(engine)}&query_label=a%2Bb+c";
            RespondWith(() => GetResponseMessage("{}", HttpStatusCode.OK,
                new Dictionary<string, string> { { "Firebolt-Update-Endpoint", endpoint } }));

            await ExecuteQuery(client, "db");
            await ExecuteQuery(client, "db");

            var parameters = SentParameters(_sentUris.Last());
            Assert.That(connection.EngineName, Is.EqualTo(engine));
            Assert.That(parameters["engine"], Is.EqualTo(engine));
            Assert.That(parameters["query_label"], Is.EqualTo("a+b c"));
        }

        [TestCaseSource(nameof(TrickyValues))]
        public async Task ParameterFromSystemEngineUrlIsDecodedAndEncodedAgain(string label)
        {
            // A fresh account per case, because the system engine URL is cached per account.
            var account = Guid.NewGuid().ToString();
            var connection = new FireboltConnection($"clientid=id;clientsecret=secret;account={account};env=mock");
            var client = new FireboltClient2(connection, Guid.NewGuid().ToString(), "secret", "", "mock", account, _httpClient);
            client.SetToken("token");
            connection.Client = client;
            var systemEngineUrl = $"https://system.mock.firebolt.io?query_label={Uri.EscapeDataString(label)}";
            RespondWith(() => GetResponseMessage(new GetSystemEngineUrlResponse { engineUrl = systemEngineUrl }, HttpStatusCode.OK));

            await client.ConnectAsync(null, "", CancellationToken.None);
            await ExecuteQuery(client, null);

            Assert.That(SentParameters(_sentUris.Last())["query_label"], Is.EqualTo(label));
        }

        [Test]
        public void FailedRepeatOfAppliedSetKeepsTheSetting()
        {
            var connection = new FireboltConnection(ConnectionString) { EngineUrl = "engine.mock.firebolt.io" };
            CreateClient(connection);
            new FireboltCommand(connection, "SET time_zone='UTC'", new FireboltParameterCollection()).ExecuteNonQuery();
            RespondWith(() => GetResponseMessage("engine unavailable", HttpStatusCode.ServiceUnavailable));

            var repeat = new FireboltCommand(connection, "SET time_zone = 'UTC'", new FireboltParameterCollection());

            Assert.Catch(() => repeat.ExecuteNonQuery());
            Assert.That(connection.SetParamList, Is.EqualTo(new HashSet<string> { "time_zone=UTC" }));
        }

        [Test]
        public void FailedNewSetIsRolledBack()
        {
            var connection = new FireboltConnection(ConnectionString) { EngineUrl = "engine.mock.firebolt.io" };
            CreateClient(connection);
            RespondWith(() => GetResponseMessage("engine unavailable", HttpStatusCode.ServiceUnavailable));

            var command = new FireboltCommand(connection, "SET time_zone='UTC'", new FireboltParameterCollection());

            Assert.Catch(() => command.ExecuteNonQuery());
            Assert.That(connection.SetParamList, Is.Empty);
        }

        [Test]
        public void SetReplacesTheValueOfAnEarlierSetWithTheSameName()
        {
            var connection = new FireboltConnection(ConnectionString) { EngineUrl = "engine.mock.firebolt.io" };
            CreateClient(connection);

            new FireboltCommand(connection, "SET time_zone='UTC'", new FireboltParameterCollection()).ExecuteNonQuery();
            new FireboltCommand(connection, "SET time_zone='Etc/GMT+1'", new FireboltParameterCollection()).ExecuteNonQuery();
            new FireboltCommand(connection, "SELECT 1", new FireboltParameterCollection()).ExecuteNonQuery();

            Assert.That(connection.SetParamList, Is.EqualTo(new HashSet<string> { "time_zone=Etc/GMT+1" }));
            Assert.That(SentParameters(_sentUris.Last()).GetValues("time_zone"), Is.EqualTo(new[] { "Etc/GMT+1" }));
        }

        [Test]
        public void FailedSetKeepsTheEarlierValueOfTheSameName()
        {
            var connection = new FireboltConnection(ConnectionString) { EngineUrl = "engine.mock.firebolt.io" };
            CreateClient(connection);
            new FireboltCommand(connection, "SET time_zone='UTC'", new FireboltParameterCollection()).ExecuteNonQuery();
            RespondWith(() => GetResponseMessage("engine unavailable", HttpStatusCode.ServiceUnavailable));

            var command = new FireboltCommand(connection, "SET time_zone='Etc/GMT+1'", new FireboltParameterCollection());

            Assert.Catch(() => command.ExecuteNonQuery());
            Assert.That(connection.SetParamList, Is.EqualTo(new HashSet<string> { "time_zone=UTC" }));
        }

        [Test]
        public void CancelledSetIsRolledBack()
        {
            var connection = new FireboltConnection(ConnectionString) { EngineUrl = "engine.mock.firebolt.io" };
            CreateClient(connection);
            using var cancellation = new CancellationTokenSource();
            RespondWith(() =>
            {
                cancellation.Cancel();
                return GetResponseMessage("{}", HttpStatusCode.OK);
            });

            var command = new FireboltCommand(connection, "SET time_zone='UTC'", new FireboltParameterCollection());

            Assert.CatchAsync<OperationCanceledException>(() => command.ExecuteNonQueryAsync(cancellation.Token));
            Assert.That(connection.SetParamList, Is.Empty);
        }

        [Test]
        public async Task SetQueryLabelReplacesTheGeneratedLabel()
        {
            var connection = new FireboltConnection(ConnectionString) { InfraVersion = 2 };
            connection.UpdateConnectionSettings(new FireboltConnectionStringBuilder(ConnectionString) { Engine = "engine" }, CancellationToken.None);
            var client = CreateClient(connection);

            await ExecuteQuery(client, "db", "query_label=mine");

            Assert.That(SentParameters(_sentUris.Single()).GetValues("query_label"), Is.EqualTo(new[] { "mine" }));
        }

        [TestCase("SET time_zone = 'Europe/Berlin'", "time_zone=Europe/Berlin")]
        [TestCase("SET time_zone='Europe/Berlin';", "time_zone=Europe/Berlin")]
        [TestCase("SET query_label = 'it''s a#b&c=d' ; ", "query_label=it's a#b&c=d")]
        [TestCase("SET time_zone=UTC+1", "time_zone=UTC+1")]
        [TestCase("SET query_label=a=b", "query_label=a=b")]
        [TestCase("SET query_label='", "query_label='")]
        [TestCase("SET query_label=''", "query_label=")]
        [TestCase("SET flag", "flag")]
        public void SetCommandIsNormalized(string commandText, string expected)
        {
            var connection = new FireboltConnection(ConnectionString) { EngineUrl = "engine.mock.firebolt.io" };
            CreateClient(connection);

            new FireboltCommand(connection, commandText, new FireboltParameterCollection()).ExecuteNonQuery();

            Assert.That(connection.SetParamList, Is.EqualTo(new HashSet<string> { expected }));
        }

        [TestCase("SET = 1")]
        [TestCase("SET ;")]
        public void SetCommandWithoutNameIsRejected(string commandText)
        {
            var connection = new FireboltConnection(ConnectionString) { EngineUrl = "engine.mock.firebolt.io" };
            CreateClient(connection);

            var command = new FireboltCommand(connection, commandText, new FireboltParameterCollection());

            Assert.Throws<InvalidOperationException>(() => command.ExecuteNonQuery());
            Assert.That(connection.SetParamList, Is.Empty);
        }

        [Test]
        public void SetCommandValueReachesTheServerUnchanged()
        {
            var connection = new FireboltConnection(ConnectionString) { EngineUrl = "engine.mock.firebolt.io" };
            CreateClient(connection);

            new FireboltCommand(connection, "SET query_label = 'a&b #c+d%20'", new FireboltParameterCollection()).ExecuteNonQuery();
            new FireboltCommand(connection, "SELECT 1", new FireboltParameterCollection()).ExecuteNonQuery();

            Assert.That(SentParameters(_sentUris.Last())["query_label"], Is.EqualTo("a&b #c+d%20"));
        }

        [TestCaseSource(nameof(TrickyValues))]
        public async Task V1DatabaseLookupIsEncoded(string database)
        {
            var client = CreateClient(new FireboltConnection(ConnectionString));
            RespondWith(() => GetResponseMessage(new GetEngineUrlByDatabaseNameResponse { Engine_url = "https://engine" }, HttpStatusCode.OK));

            await client.ConnectAsync(null, database, CancellationToken.None);

            var lookup = _sentUris.Single(uri => uri.AbsolutePath.EndsWith("engines:getURLByDatabaseName"));
            Assert.That(SentParameters(lookup)["databaseName"], Is.EqualTo(database));
        }

        [TestCaseSource(nameof(TrickyValues))]
        public void V1EngineLookupIsEncoded(string engine)
        {
            var client = CreateClient(new FireboltConnection(ConnectionString));
            RespondWith(() => GetResponseMessage(new GetEngineIdByEngineNameResponse(), HttpStatusCode.OK));

            Assert.ThrowsAsync<FireboltException>(() => client.ConnectAsync(engine, "db", CancellationToken.None));

            var lookup = _sentUris.Single(uri => uri.AbsolutePath.EndsWith("engines:getIdByName"));
            Assert.That(SentParameters(lookup)["engine_name"], Is.EqualTo(engine));
        }

        [TestCaseSource(nameof(TrickyValues))]
        public async Task V1AccountLookupIsEncoded(string account)
        {
            var client = CreateClient(new FireboltConnection(ConnectionString));
            RespondWith(() => GetResponseMessage(new GetAccountIdByNameResponse { id = "id" }, HttpStatusCode.OK));

            await client.GetAccountIdByNameAsync(account, CancellationToken.None);

            Assert.That(SentParameters(_sentUris.Single())["accountName"], Is.EqualTo(account));
        }
    }
}
