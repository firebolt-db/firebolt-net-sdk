using System.IO.Compression;
using System.Net;
using System.Reflection;
using System.Text;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Tests.Helpers;
using Moq;
using Moq.Protected;
using static FireboltDotNetSdk.Tests.Helpers.HttpResponseHelper;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    public class HttpCompressionTest
    {
        private const string ConnectionString =
            "database=db;clientid=id;clientsecret=secret;account=account;endpoint=api.mock.firebolt.io";
        private static readonly string StartLine = JsonLinesMessages.Start(("id", "integer")) + "\n";
        private const string FinishLine = JsonLinesMessages.Finish + "\n";

        [TestCase(false)]
        [TestCase(true)]
        public async Task Query_LeavesCompressionToTheRequestHeader(bool streaming)
        {
            var uri = await CapturedQueryUri(ConnectionString, new HashSet<string>(), streaming);
            Assert.That(uri.Query, Does.Not.Contain("enable_http_compression"));
        }

        [TestCase(null, true)]
        [TestCase("false", false)]
        public void Connection_UsesTheSharedHttpClientMatchingCompression(string? value, bool compression)
        {
            var connection = new FireboltConnection(value == null ? ConnectionString : $"{ConnectionString};compression={value}");
            var client = typeof(FireboltConnection).GetMethod("CreateClient", BindingFlags.NonPublic | BindingFlags.Instance)!
                .Invoke(connection, null);
            var httpClient = typeof(FireboltClient).GetField("_httpClient", BindingFlags.NonPublic | BindingFlags.Instance)!
                .GetValue(client);
            Assert.That(httpClient, Is.SameAs(HttpClientSingleton.GetInstance(compression)));
        }

        [TestCase(null, true)]
        [TestCase("true", true)]
        [TestCase("False", false)]
        public void ConnectionString_Compression_IsParsed(string? value, bool expected)
        {
            var connectionString = value == null ? ConnectionString : $"{ConnectionString};Compression={value}";
            Assert.Multiple(() =>
            {
                Assert.That(new FireboltConnectionStringBuilder(connectionString).Compression, Is.EqualTo(expected));
                Assert.That(new FireboltConnection(connectionString).IsCompressionEnabled, Is.EqualTo(expected));
            });
        }

        [Test]
        public void ConnectionStringBuilder_SetCompression_IsWrittenToTheConnectionString()
        {
            var builder = new FireboltConnectionStringBuilder(ConnectionString) { Compression = false };
            Assert.That(new FireboltConnectionStringBuilder(builder.ConnectionString).Compression, Is.False);
        }

        [TestCase("1", true)]
        [TestCase("0", false)]
        [TestCase(" TRUE ", true)]
        public void ConnectionString_CompressionAsNumberOrAnyCase_IsParsed(string value, bool expected)
        {
            Assert.That(new FireboltConnectionStringBuilder($"{ConnectionString};compression={value}").Compression, Is.EqualTo(expected));
        }

        [TestCase("0", false)]
        [TestCase("1", true)]
        public void ConnectionString_CacheConnectionAsNumber_IsParsed(string value, bool expected)
        {
            Assert.That(new FireboltConnectionStringBuilder($"{ConnectionString};cacheconnection={value}").CacheConnection, Is.EqualTo(expected));
        }

        [Test]
        public void ConnectionString_InvalidCompression_ThrowsNamingTheOption()
        {
            var exception = Assert.Catch<FormatException>(() => _ = new FireboltConnection(ConnectionString + ";compression=maybe"));
            Assert.That(exception!.Message, Is.EqualTo("Invalid value 'maybe' for Compression, expected true, false, 1 or 0."));
        }

        [Test]
        public void ConnectionString_ChangingOnlyCompression_TakesEffect()
        {
            var connection = new FireboltConnection(ConnectionString);
            connection.ConnectionString = ConnectionString + ";compression=false";
            Assert.That(connection.IsCompressionEnabled, Is.False);
        }

        [Test]
        public async Task SharedHttpClient_GzipResponse_IsAcceptedAndDecompressedWhileStreaming()
        {
            await using var server = new LoopbackHttpServer();
            var responseTask = HttpClientSingleton.GetInstance().GetAsync(server.Url, HttpCompletionOption.ResponseHeadersRead);
            Assert.That(await server.RequestHead, Does.Match("(?im)^Accept-Encoding:.*gzip"));

            var compressed = new MemoryStream();
            var gzip = new GZipStream(compressed, CompressionLevel.Fastest, leaveOpen: true);
            await server.WriteAsync("HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n\r\n");
            await server.WriteChunkAsync(Compress(gzip, compressed, StartLine + DataLine(0, 2), finish: false));

            using var response = await responseTask;
            using var reader = new FireboltStreamingDataReader(new StreamReader(await response.Content.ReadAsStreamAsync()));
            Assert.Multiple(() =>
            {
                Assert.That(reader.Read(), Is.True, "rows must be readable before the compressed body is complete");
                Assert.That(reader.GetInt32(0), Is.EqualTo(0));
                Assert.That(reader.Read(), Is.True);
            });

            await server.WriteChunkAsync(Compress(gzip, compressed, DataLine(2, 1) + FinishLine, finish: true));
            await server.WriteAsync("0\r\n\r\n");
            Assert.Multiple(() =>
            {
                Assert.That(reader.Read(), Is.True);
                Assert.That(reader.GetInt32(0), Is.EqualTo(2));
                Assert.That(reader.Read(), Is.False);
            });
        }

        [Test]
        public async Task SharedHttpClient_WithoutCompression_DoesNotAcceptGzip()
        {
            await using var server = new LoopbackHttpServer();
            _ = HttpClientSingleton.GetInstance(compression: false).GetAsync(server.Url);
            Assert.That(await server.RequestHead, Does.Not.Match("(?im)^Accept-Encoding:"));
        }

        [Test]
        public async Task SharedHttpClient_GzipErrorResponse_IsDecompressed()
        {
            // The SDK reads error bodies as text, so they must arrive decompressed like results do.
            const string error = "{\"errors\":[{\"description\":\"Line 1, Column 8: syntax error\"}]}";
            await using var server = new LoopbackHttpServer();
            var responseTask = HttpClientSingleton.GetInstance().GetAsync(server.Url);
            await server.RequestHead;

            var compressed = new MemoryStream();
            using (var gzip = new GZipStream(compressed, CompressionLevel.Fastest, leaveOpen: true))
                gzip.Write(Encoding.UTF8.GetBytes(error));
            await server.WriteAsync($"HTTP/1.1 400 Bad Request\r\nContent-Encoding: gzip\r\nContent-Length: {compressed.Length}\r\n\r\n");
            await server.WriteAsync(compressed.ToArray());

            using var response = await responseTask;
            Assert.Multiple(async () =>
            {
                Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.BadRequest));
                Assert.That(await response.Content.ReadAsStringAsync(), Is.EqualTo(error));
            });
        }

        private static async Task<Uri> CapturedQueryUri(string connectionString, HashSet<string> setParamList, bool streaming)
        {
            var (handlerMock, httpClient) = FireboltClientTest.GetHttpMocks();
            var client = CreateClient(new FireboltConnection(connectionString), httpClient);

            Uri? captured = null;
            handlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Callback((HttpRequestMessage request, CancellationToken _) => captured = request.RequestUri)
                .ReturnsAsync(() => GetResponseMessage(StartLine + FinishLine, HttpStatusCode.OK));

            if (streaming)
            {
                using var _ = await client.ExecuteQueryAsync<StreamReader>("engine.mock.firebolt.io", "db", null, "select 1",
                    setParamList, CancellationToken.None);
            }
            else
            {
                await client.ExecuteQueryAsync<string>("engine.mock.firebolt.io", "db", null, "select 1",
                    setParamList, CancellationToken.None);
            }
            return captured!;
        }

        private static FireboltClient CreateClient(FireboltConnection connection, HttpClient httpClient)
        {
            var client = new FireboltClient1(connection, Guid.NewGuid().ToString(), "secret", "http://api.mock.firebolt.io",
                null, "account", httpClient);
            client.SetToken("token");
            return client;
        }

        private static byte[] Compress(GZipStream gzip, MemoryStream compressed, string text, bool finish)
        {
            var bytes = Encoding.UTF8.GetBytes(text);
            gzip.Write(bytes, 0, bytes.Length);
            if (finish) gzip.Dispose();
            else gzip.Flush();
            var chunk = compressed.ToArray();
            compressed.SetLength(0);
            return chunk;
        }

        private static string DataLine(int firstId, int rowCount)
        {
            return JsonLinesMessages.Data(Enumerable.Range(firstId, rowCount).Select(id => new object?[] { id })) + "\n";
        }
    }
}
