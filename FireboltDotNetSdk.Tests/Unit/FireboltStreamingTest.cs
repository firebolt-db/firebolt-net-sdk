using System.Net;
using System.Text;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using FireboltNETSDK.Exception;
using Moq;
using Moq.Protected;
using static FireboltDotNetSdk.Client.FireResponse;
using FireboltDotNetSdk.Tests.Helpers;
using static FireboltDotNetSdk.Tests.Helpers.HttpResponseHelper;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    public class FireboltStreamingTest
    {
        private static readonly string StartLine = JsonLinesMessages.Start(("id", "integer"), ("payload", "text"));
        private const string FinishLine = JsonLinesMessages.Finish;
        private static readonly string ErrorLine = JsonLinesMessages.FinishWithErrors("division by zero");
        private static readonly string Payload = new('x', 230);

        private Mock<HttpMessageHandler> _handlerMock = null!;
        private FireboltConnection _connection = null!;

        [SetUp]
        public void SetUp()
        {
            var (handlerMock, httpClient) = FireboltClientTest.GetHttpMocks();
            _handlerMock = handlerMock;
            // Fresh client and account names keep the process-wide token and account caches from skipping
            // the login and account lookup requests, so every test sees the same request sequence.
            var account = "account_" + Guid.NewGuid().ToString("N");
            _connection = new FireboltConnection($"database=db;clientid=id;clientsecret=secret;account={account};endpoint=api.mock.firebolt.io")
            {
                EngineUrl = "https://engine.mock.firebolt.io"
            };
            _connection.Client = new FireboltClient1(_connection, Guid.NewGuid().ToString(), "secret",
                "http://api.mock.firebolt.io", null, account, httpClient);
        }

        [TearDown]
        public void TearDown()
        {
            _connection.Dispose();
        }

        [Test]
        public void ExecuteStreamedQuery_ReturnsBeforeTheBodyIsComplete()
        {
            using var releaseRest = new ManualResetEventSlim(false);
            var stream = new JsonLinesStream(GatedLines(releaseRest));
            RespondWith(StreamResponse(stream));

            try
            {
                var execution = CreateCommand().ExecuteStreamedQueryAsync();
                Assert.That(execution.Wait(TimeSpan.FromSeconds(10)), Is.True,
                    "ExecuteStreamedQueryAsync must not wait for the whole response body");

                using var reader = execution.Result;
                Assert.Multiple(() =>
                {
                    Assert.That(reader.Read(), Is.True);
                    Assert.That(reader.GetInt32(0), Is.EqualTo(0));
                });

                releaseRest.Set();
                var rows = 1;
                while (reader.Read()) rows++;
                Assert.That(rows, Is.EqualTo(4));
            }
            finally
            {
                releaseRest.Set();
            }
        }

        [Test]
        public async Task ExecuteStreamedQuery_LargeResult_KeepsMemoryBounded()
        {
            const int rowCount = 400_000;
            var stream = new JsonLinesStream(ResultLines(rowCount, rowsPerLine: 1_000));
            RespondWith(StreamResponse(stream));

            var baseline = GC.GetTotalMemory(forceFullCollection: true);
            long peak = 0;
            var rows = 0;
            await using (var reader = await CreateCommand().ExecuteStreamedQueryAsync())
            {
                while (reader.Read())
                {
                    Assert.That(reader.GetInt32(0), Is.EqualTo(rows));
                    if (rows++ % 50_000 == 0)
                        peak = Math.Max(peak, GC.GetTotalMemory(forceFullCollection: true) - baseline);
                }
            }

            Assert.Multiple(() =>
            {
                Assert.That(rows, Is.EqualTo(rowCount));
                Assert.That(stream.BytesProduced, Is.GreaterThan(90L * 1024 * 1024));
                Assert.That(peak, Is.LessThan(32L * 1024 * 1024),
                    $"Managed heap grew by {peak / 1024 / 1024} MB while streaming {stream.BytesProduced / 1024 / 1024} MB");
            });
        }

        [Test]
        public void ExecuteStreamedQuery_ErrorStatus_ThrowsStructuredErrorAndReleasesResponse()
        {
            var stream = new JsonLinesStream(new[] { "{\"errors\":[{\"description\":\"syntax error\"}]}" });
            RespondWith(StreamResponse(stream, HttpStatusCode.BadRequest));

            var exception = Assert.ThrowsAsync<FireboltStructuredException>(() => CreateCommand().ExecuteStreamedQueryAsync());
            Assert.Multiple(() =>
            {
                Assert.That(exception!.Message, Does.Contain("syntax error"));
                Assert.That(stream.IsDisposed, Is.True);
            });
        }

        [Test]
        public async Task ExecuteStreamedQuery_RetriedAfterUnauthorized_ReleasesTheRejectedResponse()
        {
            var rejected = new JsonLinesStream(new[] { "{\"error\":\"token expired\"}" });
            var accepted = new JsonLinesStream(ResultLines(2, rowsPerLine: 2));
            RespondWith(StreamResponse(rejected, HttpStatusCode.Unauthorized), LoginResponse(), StreamResponse(accepted));

            await using var reader = await CreateCommand().ExecuteStreamedQueryAsync();
            Assert.Multiple(() =>
            {
                Assert.That(rejected.IsDisposed, Is.True);
                Assert.That(accepted.IsDisposed, Is.False);
                Assert.That(reader.Read(), Is.True);
            });
        }

        [Test]
        public void ExecuteStreamedQuery_PlainJsonErrorWithOkStatus_ThrowsStructuredErrorAndReleasesStream()
        {
            var stream = new JsonLinesStream(new[] { "{\"errors\":[{\"description\":\"out of memory\"}]}" });
            RespondWith(StreamResponse(stream));

            var exception = Assert.ThrowsAsync<FireboltStructuredException>(() => CreateCommand().ExecuteStreamedQueryAsync());
            Assert.Multiple(() =>
            {
                Assert.That(exception!.Message, Does.Contain("out of memory"));
                Assert.That(stream.IsDisposed, Is.True);
            });
        }

        [Test]
        public async Task ExecuteStreamedQuery_ErrorAfterRows_IsRaisedByRead()
        {
            var stream = new JsonLinesStream(new[] { StartLine, DataLine(0, 2), ErrorLine });
            RespondWith(StreamResponse(stream));

            await using var reader = await CreateCommand().ExecuteStreamedQueryAsync();
            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.Read(), Is.True);
            var exception = Assert.Throws<FireboltStructuredException>(() => reader.Read());
            Assert.That(exception!.Message, Does.Contain("division by zero"));
        }

        [Test]
        public async Task Close_BeforeTheEnd_ReleasesTheResponseStream()
        {
            var stream = new JsonLinesStream(ResultLines(10_000, rowsPerLine: 100));
            RespondWith(StreamResponse(stream));

            var reader = await CreateCommand().ExecuteStreamedQueryAsync();
            Assert.That(reader.Read(), Is.True);
            reader.Close();

            Assert.Multiple(() =>
            {
                Assert.That(stream.IsDisposed, Is.True);
                Assert.That(stream.BytesProduced, Is.LessThan(1024 * 1024));
                Assert.That(reader.IsClosed, Is.True);
                Assert.That(reader.Read(), Is.False);
            });
        }

        [Test]
        public async Task Read_AfterTheLastRow_KeepsReturningFalse()
        {
            RespondWith(StreamResponse(new JsonLinesStream(ResultLines(1, rowsPerLine: 1))));

            await using var reader = await CreateCommand().ExecuteStreamedQueryAsync();
            Assert.Multiple(() =>
            {
                Assert.That(reader.Read(), Is.True);
                Assert.That(reader.Read(), Is.False);
                Assert.That(reader.Read(), Is.False);
            });
        }

        [Test]
        public async Task Read_StreamEndsWithoutFinishMessage_Throws()
        {
            RespondWith(StreamResponse(new JsonLinesStream(new[] { StartLine, DataLine(0, 2) })));

            await using var reader = await CreateCommand().ExecuteStreamedQueryAsync();
            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.Read(), Is.True);
            var exception = Assert.Throws<FireboltException>(() => reader.Read());
            Assert.That(exception!.Message, Does.Contain("Stream ended before the server signalled the end of the result"));
        }

        [Test]
        public async Task JsonLines_RowsSpreadOverManyDataLines_AreReturnedInOrder()
        {
            var lines = new[] { StartLine, DataLine(0, 3), DataLine(3, 1), DataLine(4, 500), FinishLine };
            RespondWith(StreamResponse(new JsonLinesStream(lines)));

            await using var reader = await CreateCommand().ExecuteStreamedQueryAsync();
            var ids = new List<int>();
            while (await reader.ReadAsync())
            {
                ids.Add(reader.GetInt32(0));
                Assert.That(reader.GetString(1), Is.EqualTo(Payload));
            }
            Assert.That(ids, Is.EqualTo(Enumerable.Range(0, 504)));
        }

        [Test]
        public async Task JsonLines_TypedColumnsAndNulls_AreConverted()
        {
            var lines = new[]
            {
                "{\"message_type\":\"START\",\"result_columns\":[{\"name\":\"i\",\"type\":\"integer\"},{\"name\":\"l\",\"type\":\"bigint\"}," +
                "{\"name\":\"d\",\"type\":\"double precision\"},{\"name\":\"b\",\"type\":\"boolean\"},{\"name\":\"t\",\"type\":\"text null\"}]}",
                "{\"message_type\":\"DATA\",\"data\":[[1,9007199254740993,1.5,true,\"a\"],[2,-1,-0.25,false,null]]}",
                FinishLine
            };
            RespondWith(StreamResponse(new JsonLinesStream(lines)));

            await using var reader = await CreateCommand().ExecuteStreamedQueryAsync();
            Assert.Multiple(() =>
            {
                Assert.That(reader.FieldCount, Is.EqualTo(5));
                Assert.That(reader.GetName(4), Is.EqualTo("t"));
                Assert.That(reader.Read(), Is.True);
                Assert.That(reader.GetInt32(0), Is.EqualTo(1));
                Assert.That(reader.GetInt64(1), Is.EqualTo(9007199254740993L));
                Assert.That(reader.GetDouble(2), Is.EqualTo(1.5));
                Assert.That(reader.GetBoolean(3), Is.True);
                Assert.That(reader.GetString(4), Is.EqualTo("a"));
                Assert.That(reader.Read(), Is.True);
                Assert.That(reader.GetInt64(1), Is.EqualTo(-1L));
                Assert.That(reader.GetBoolean(3), Is.False);
                Assert.That(reader.IsDBNull(4), Is.True);
                Assert.That(reader.Read(), Is.False);
            });
        }

        [Test]
        public async Task JsonLines_EmptyResult_HasNoRows()
        {
            RespondWith(StreamResponse(new JsonLinesStream(new[] { StartLine, FinishLine })));

            await using var reader = await CreateCommand().ExecuteStreamedQueryAsync();
            Assert.Multiple(() =>
            {
                Assert.That(reader.FieldCount, Is.EqualTo(2));
                Assert.That(reader.HasRows, Is.False);
                Assert.That(reader.Read(), Is.False);
            });
        }

        [Test]
        public async Task JsonLines_SingleRow_HasRowsAfterReadingIt()
        {
            RespondWith(StreamResponse(new JsonLinesStream(ResultLines(1, rowsPerLine: 1))));

            await using var reader = await CreateCommand().ExecuteStreamedQueryAsync();
            Assert.Multiple(() =>
            {
                Assert.That(reader.HasRows, Is.True);
                Assert.That(reader.Read(), Is.True);
                Assert.That(reader.HasRows, Is.True);
            });
        }

        [Test]
        public async Task JsonLines_ErrorAfterSeveralDataLines_ReturnsEveryEarlierRowThenThrows()
        {
            var lines = new[] { StartLine, DataLine(0, 1_000), DataLine(1_000, 1_000), DataLine(2_000, 1_000), ErrorLine };
            var stream = new JsonLinesStream(lines);
            RespondWith(StreamResponse(stream));

            var reader = await CreateCommand().ExecuteStreamedQueryAsync();
            var rows = 0;
            var exception = Assert.Throws<FireboltStructuredException>(() =>
            {
                while (reader.Read())
                    Assert.That(reader.GetInt32(0), Is.EqualTo(rows++));
            });
            Assert.Multiple(() =>
            {
                Assert.That(rows, Is.EqualTo(3_000));
                Assert.That(exception!.Message, Does.Contain("division by zero"));
                Assert.That(reader.Read(), Is.False, "the reader must stay finished after reporting the error");
            });
            await reader.DisposeAsync();
            Assert.That(stream.IsDisposed, Is.True);
        }

        [Test]
        public async Task JsonLines_LargeResultFailingAtTheEnd_StreamsRowsBeforeTheErrorArrives()
        {
            const int rowCount = 200_000;
            var stream = new JsonLinesStream(ResultLines(rowCount, rowsPerLine: 1_000).SkipLast(1).Append(ErrorLine));
            RespondWith(StreamResponse(stream));

            await using var reader = await CreateCommand().ExecuteStreamedQueryAsync();
            Assert.Multiple(() =>
            {
                Assert.That(reader.Read(), Is.True);
                Assert.That(stream.BytesProduced, Is.LessThan(1024 * 1024), "rows must be readable before the error at the end is received");
            });

            var rows = 1;
            Assert.Multiple(() =>
            {
                Assert.Throws<FireboltStructuredException>(() =>
                {
                    while (reader.Read()) rows++;
                });
                Assert.That(rows, Is.EqualTo(rowCount));
            });
        }

        [Test]
        public async Task JsonLines_UnknownMessageTypeMidStream_Throws()
        {
            RespondWith(StreamResponse(new JsonLinesStream(new[] { StartLine, DataLine(0, 1), "{\"message_type\":\"PROGRESS\"}", FinishLine })));

            await using var reader = await CreateCommand().ExecuteStreamedQueryAsync();
            Assert.That(reader.Read(), Is.True);
            var exception = Assert.Throws<FireboltException>(() => reader.Read());
            Assert.That(exception!.Message, Does.Contain("Unexpected messageType: PROGRESS"));
        }

        [Test]
        public async Task Cancel_WhileReadIsWaitingForData_AbortsTheRead()
        {
            var stream = new JsonLinesStream(new[] { StartLine, DataLine(0, 1) }, stallAtEnd: true);
            RespondWith(StreamResponse(stream));
            using var cancellation = new CancellationTokenSource();

            await using var reader = await CreateCommand(commandTimeoutMillis: 0).ExecuteStreamedQueryAsync(cancellation.Token);
            Assert.That(reader.Read(), Is.True);
            var blockedRead = Task.Run(() => reader.Read());
            cancellation.CancelAfter(100);

            Assert.Multiple(() =>
            {
                Assert.That(() => blockedRead.Wait(TimeSpan.FromSeconds(10)),
                    Throws.InstanceOf<AggregateException>().With.InnerException.InstanceOf<OperationCanceledException>());
                Assert.That(stream.IsDisposed, Is.True);
            });
        }

        [Test]
        public async Task Cancel_BetweenReads_StopsTheNextRead()
        {
            RespondWith(StreamResponse(new JsonLinesStream(ResultLines(10_000, rowsPerLine: 100))));
            using var cancellation = new CancellationTokenSource();

            await using var reader = await CreateCommand().ExecuteStreamedQueryAsync(cancellation.Token);
            Assert.That(reader.Read(), Is.True);
            cancellation.Cancel();

            // Rows already parsed from the current DATA line may still be served; fetching more must stop.
            Assert.Multiple(() =>
            {
                Assert.Throws<OperationCanceledException>(() =>
                {
                    while (reader.Read()) { }
                });
                Assert.That(reader.Read(), Is.False);
            });
        }

        [Test]
        public async Task CommandTimeout_ServerStopsSendingMidStream_ThrowsTimeout()
        {
            var stream = new JsonLinesStream(new[] { StartLine, DataLine(0, 1) }, stallAtEnd: true);
            RespondWith(StreamResponse(stream));

            await using var reader = await CreateCommand(commandTimeoutMillis: 200).ExecuteStreamedQueryAsync();
            Assert.That(reader.Read(), Is.True);
            var read = Task.Run(() => reader.Read());

            Assert.Multiple(() =>
            {
                Assert.That(() => read.Wait(TimeSpan.FromSeconds(10)),
                    Throws.InstanceOf<AggregateException>().With.InnerException.InstanceOf<FireboltTimeoutException>());
                Assert.That(stream.IsDisposed, Is.True);
            });
        }

        [Test]
        public async Task CommandTimeout_SteadyStreamLongerThanTheTimeout_IsReadCompletely()
        {
            RespondWith(StreamResponse(new JsonLinesStream(SlowLines(lineCount: 8, delayMillis: 100))));

            await using var reader = await CreateCommand(commandTimeoutMillis: 400).ExecuteStreamedQueryAsync();
            var rows = 0;
            while (reader.Read()) rows++;
            Assert.That(rows, Is.EqualTo(8));
        }

        [Test]
        public async Task CommandTimeout_StalledRealHttpResponse_AbortsTheSocketRead()
        {
            // A real socket shows that cancelling the read's token makes SocketsHttpHandler abort a read waiting on the network.
            await using var server = new LoopbackHttpServer();
            using var httpClient = new HttpClient(new SocketsHttpHandler());
            var responseTask = httpClient.GetAsync(server.Url, HttpCompletionOption.ResponseHeadersRead);
            await server.RequestHead;
            await server.WriteAsync("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
            await server.WriteChunkAsync(Encoding.UTF8.GetBytes(StartLine + "\n" + DataLine(0, 1) + "\n"));

            using var response = await responseTask;
            var streamReader = new StreamReader(await response.Content.ReadAsStreamAsync());
            using var reader = await FireboltStreamingDataReader.CreateAsync(streamReader, 300, CancellationToken.None);
            Assert.That(reader.Read(), Is.True);
            var elapsed = System.Diagnostics.Stopwatch.StartNew();
            var read = Task.Run(() => reader.Read());

            Assert.Multiple(() =>
            {
                Assert.That(() => read.Wait(TimeSpan.FromSeconds(10)),
                    Throws.InstanceOf<AggregateException>().With.InnerException.InstanceOf<FireboltTimeoutException>());
                // Aborting through the token closes the connection at once instead of waiting for a response drain.
                Assert.That(elapsed.ElapsedMilliseconds, Is.LessThan(2_000));
            });
        }

        [Test]
        public void CommandTimeout_ServerSendsNothingAfterTheHeaders_ExecuteTimesOutAndReleasesTheStream()
        {
            var stream = new JsonLinesStream(Array.Empty<string>(), stallAtEnd: true);
            RespondWith(StreamResponse(stream));

            Assert.Multiple(() =>
            {
                Assert.ThrowsAsync<FireboltTimeoutException>(() => CreateCommand(commandTimeoutMillis: 200).ExecuteStreamedQueryAsync());
                Assert.That(stream.IsDisposed, Is.True);
            });
        }

        [Test]
        public void CommandTimeout_ServerSendsNothingAfterTheHeaders_SynchronousExecuteTimesOut()
        {
            var stream = new JsonLinesStream(Array.Empty<string>(), stallAtEnd: true);
            RespondWith(StreamResponse(stream));

            Assert.Multiple(() =>
            {
                Assert.Throws<FireboltTimeoutException>(() => CreateCommand(commandTimeoutMillis: 200).ExecuteStreamedQuery());
                Assert.That(stream.IsDisposed, Is.True);
            });
        }

        [Test]
        public void Cancel_BeforeTheFirstLineArrives_ExecuteThrowsAndReleasesTheStream()
        {
            var stream = new JsonLinesStream(Array.Empty<string>(), stallAtEnd: true);
            RespondWith(StreamResponse(stream));
            using var cancellation = new CancellationTokenSource();
            cancellation.CancelAfter(100);

            Assert.Multiple(() =>
            {
                Assert.CatchAsync<OperationCanceledException>(() =>
                    CreateCommand(commandTimeoutMillis: 0).ExecuteStreamedQueryAsync(cancellation.Token));
                Assert.That(stream.IsDisposed, Is.True);
            });
        }

        [Test]
        public async Task Cancel_WhileATimeoutIsConfigured_IsReportedAsCancellation()
        {
            RespondWith(StreamResponse(new JsonLinesStream(new[] { StartLine, DataLine(0, 1) }, stallAtEnd: true)));
            using var cancellation = new CancellationTokenSource();

            await using var reader = await CreateCommand(commandTimeoutMillis: 10_000).ExecuteStreamedQueryAsync(cancellation.Token);
            Assert.That(reader.Read(), Is.True);
            var blockedRead = Task.Run(() => reader.Read());
            cancellation.CancelAfter(100);

            var exception = Assert.Throws<AggregateException>(() => blockedRead.Wait(TimeSpan.FromSeconds(10)));
            Assert.That(exception!.InnerException, Is.InstanceOf<OperationCanceledException>());
        }

        [Test]
        public void Read_AfterExecuteSynchronously_StreamsOverHttp()
        {
            RespondWith(StreamResponse(new JsonLinesStream(ResultLines(3, rowsPerLine: 2))));

            using var reader = CreateCommand().ExecuteStreamedQuery();
            var rows = 0;
            while (reader.Read()) Assert.That(reader.GetInt32(0), Is.EqualTo(rows++));
            Assert.That(rows, Is.EqualTo(3));
        }

        [TestCase(HttpStatusCode.InternalServerError, "Internal error: engine restarted")]
        [TestCase(HttpStatusCode.ServiceUnavailable, "")]
        public void ExecuteStreamedQuery_ErrorStatusWithoutStructuredBody_ThrowsWithStatusAndBody(HttpStatusCode status, string body)
        {
            var stream = new JsonLinesStream(body.Length > 0 ? new[] { body } : Array.Empty<string>());
            RespondWith(StreamResponse(stream, status));

            var exception = Assert.ThrowsAsync<FireboltException>(() => CreateCommand().ExecuteStreamedQueryAsync());
            Assert.Multiple(() =>
            {
                Assert.That(exception, Is.Not.InstanceOf<FireboltStructuredException>());
                Assert.That(exception!.Message, Does.Contain($"Status: {(int)status}"));
                Assert.That(exception.Message, Does.Contain(body));
                Assert.That(stream.IsDisposed, Is.True);
            });
        }

        [Test]
        public void ExecuteStreamedQuery_UnauthorizedAfterTokenRefresh_ThrowsAndReleasesBothResponses()
        {
            var first = new JsonLinesStream(new[] { "{\"error\":\"token expired\"}" });
            var second = new JsonLinesStream(new[] { "{\"error\":\"still unauthorized\"}" });
            RespondWith(StreamResponse(first, HttpStatusCode.Unauthorized), LoginResponse(), StreamResponse(second, HttpStatusCode.Unauthorized));

            var exception = Assert.ThrowsAsync<FireboltException>(() => CreateCommand().ExecuteStreamedQueryAsync());
            Assert.Multiple(() =>
            {
                Assert.That(exception!.Message, Does.Contain("The operation is unauthorized"));
                Assert.That(first.IsDisposed, Is.True);
                Assert.That(second.IsDisposed, Is.True);
            });
        }

        [Test]
        public async Task Close_CalledTwice_IsHarmless()
        {
            var stream = new JsonLinesStream(ResultLines(10, rowsPerLine: 5));
            RespondWith(StreamResponse(stream));

            var reader = await CreateCommand().ExecuteStreamedQueryAsync();
            reader.Close();
            Assert.Multiple(() =>
            {
                Assert.DoesNotThrow(() => reader.Close());
                Assert.DoesNotThrowAsync(() => reader.DisposeAsync().AsTask());
                Assert.That(stream.IsDisposed, Is.True);
            });
        }

        [Test]
        public async Task Read_ToTheEndWithoutDisposing_ReleasesTheStream()
        {
            var stream = new JsonLinesStream(ResultLines(3, rowsPerLine: 2));
            RespondWith(StreamResponse(stream));

            var reader = await CreateCommand().ExecuteStreamedQueryAsync();
            while (reader.Read()) { }
            Assert.That(stream.IsDisposed, Is.True, "a reader that is never disposed must not keep the connection");
        }

        [Test]
        public async Task Read_UntilAnErrorWithoutDisposing_ReleasesTheStream()
        {
            var stream = new JsonLinesStream(new[] { StartLine, DataLine(0, 1), ErrorLine });
            RespondWith(StreamResponse(stream));

            var reader = await CreateCommand().ExecuteStreamedQueryAsync();
            Assert.Multiple(() =>
            {
                Assert.Throws<FireboltStructuredException>(() =>
                {
                    while (reader.Read()) { }
                });
                Assert.That(stream.IsDisposed, Is.True);
            });
        }

        [Test]
        public async Task Close_FromAnotherThreadWhileAReadWaits_EndsTheReadAndReleasesTheStream()
        {
            var stream = new JsonLinesStream(new[] { StartLine, DataLine(0, 1) }, stallAtEnd: true);
            RespondWith(StreamResponse(stream));

            var reader = await CreateCommand(commandTimeoutMillis: 0).ExecuteStreamedQueryAsync();
            Assert.That(reader.Read(), Is.True);
            var blockedRead = reader.ReadAsync();
            Assert.Multiple(() =>
            {
                Assert.That(blockedRead.IsCompleted, Is.False);
                Assert.That(Task.Run(reader.Close).Wait(TimeSpan.FromSeconds(10)), Is.True, "Close must not wait for the server");
                Assert.CatchAsync<OperationCanceledException>(() => blockedRead);
                Assert.That(stream.IsDisposed, Is.True);
                Assert.That(reader.IsClosed, Is.True);
            });
        }

        [Test]
        public async Task Read_ConnectionBreaksMidStream_ThrowsFireboltExceptionAndStaysFinished()
        {
            var broken = new IOException("The response ended prematurely.");
            var stream = new JsonLinesStream(new[] { StartLine, DataLine(0, 1) }, failAtEnd: broken);
            RespondWith(StreamResponse(stream));

            await using var reader = await CreateCommand().ExecuteStreamedQueryAsync();
            Assert.That(reader.Read(), Is.True);
            var exception = Assert.Throws<FireboltException>(() => reader.Read());
            Assert.Multiple(() =>
            {
                Assert.That(exception!.InnerException, Is.SameAs(broken));
                Assert.That(reader.Read(), Is.False);
                Assert.That(stream.IsDisposed, Is.True);
            });
        }

        [Test]
        public async Task ReadAsync_WhileWaitingForTheServer_DoesNotBlockTheCaller()
        {
            RespondWith(StreamResponse(new JsonLinesStream(new[] { StartLine, DataLine(0, 1) }, stallAtEnd: true)));

            await using var reader = await CreateCommand(commandTimeoutMillis: 0).ExecuteStreamedQueryAsync();
            Assert.That(await reader.ReadAsync(), Is.True);
            using var cancellation = new CancellationTokenSource();
            var waiting = reader.ReadAsync(cancellation.Token);

            Assert.That(waiting.IsCompleted, Is.False, "ReadAsync must return to the caller while it waits");
            cancellation.Cancel();
            Assert.CatchAsync<OperationCanceledException>(() => waiting);
        }

        [Test]
        public void ExecuteStreamedQueryAsync_WhileWaitingForTheFirstRows_DoesNotBlockTheCaller()
        {
            RespondWith(StreamResponse(new JsonLinesStream(new[] { StartLine }, stallAtEnd: true)));
            using var cancellation = new CancellationTokenSource();

            var execution = CreateCommand(commandTimeoutMillis: 0).ExecuteStreamedQueryAsync(cancellation.Token);
            Assert.That(execution.IsCompleted, Is.False);
            cancellation.Cancel();
            Assert.CatchAsync<OperationCanceledException>(() => execution);
        }

        [Test]
        public async Task Read_StreamCutOffInsideALine_ThrowsFireboltExceptionAndReleasesTheStream()
        {
            var stream = new JsonLinesStream(new[] { StartLine, DataLine(0, 1), "{\"message_type\":\"DATA\",\"data\":[[1," });
            RespondWith(StreamResponse(stream));

            var reader = await CreateCommand().ExecuteStreamedQueryAsync();
            Assert.That(reader.Read(), Is.True);
            var exception = Assert.Throws<FireboltException>(() => reader.Read());
            Assert.Multiple(() =>
            {
                Assert.That(exception!.Message, Does.StartWith("Failed to parse JSON from stream on line"));
                Assert.That(stream.IsDisposed, Is.True, "the connection must be released without disposing the reader");
                Assert.That(reader.Read(), Is.False);
            });
        }

        [Test]
        public async Task Close_CalledConcurrently_ShutsDownOnce()
        {
            var stream = new JsonLinesStream(new[] { StartLine, DataLine(0, 1) }, stallAtEnd: true);
            RespondWith(StreamResponse(stream));

            var reader = await CreateCommand(commandTimeoutMillis: 0).ExecuteStreamedQueryAsync();
            using var start = new ManualResetEventSlim(false);
            var closes = Enumerable.Range(0, 16).Select(_ => Task.Run(() =>
            {
                start.Wait();
                reader.Close();
            })).ToArray();
            start.Set();

            Assert.Multiple(() =>
            {
                Assert.DoesNotThrow(() => Task.WaitAll(closes, TimeSpan.FromSeconds(10)));
                Assert.That(reader.IsClosed, Is.True);
                Assert.That(stream.IsDisposed, Is.True);
                Assert.That(reader.Read(), Is.False);
            });
        }

        private FireboltCommand CreateCommand(int commandTimeoutMillis = 30_000)
        {
            return new FireboltCommand(_connection, "SELECT id, payload FROM big_table", new FireboltParameterCollection())
            {
                CommandTimeoutMillis = commandTimeoutMillis
            };
        }

        private void RespondWith(params HttpResponseMessage[] queryResponses)
        {
            var responses = new Queue<HttpResponseMessage>(new[] { LoginResponse(), AccountIdResponse() }.Concat(queryResponses));
            _handlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync((HttpRequestMessage request, CancellationToken _) =>
                    responses.TryDequeue(out var response)
                        ? response
                        : throw new AssertionException($"Unexpected request {request.Method} {request.RequestUri}"));
        }

        private static HttpResponseMessage LoginResponse()
        {
            return GetResponseMessage(new LoginResponse("access_token", "3600", "Bearer"), HttpStatusCode.OK);
        }

        private static HttpResponseMessage AccountIdResponse()
        {
            return GetResponseMessage(new GetAccountIdByNameResponse { id = "account_id" }, HttpStatusCode.OK);
        }

        private static HttpResponseMessage StreamResponse(Stream stream, HttpStatusCode statusCode = HttpStatusCode.OK)
        {
            return new HttpResponseMessage(statusCode) { Content = new StreamContent(stream) };
        }

        private static IEnumerable<string> ResultLines(int rowCount, int rowsPerLine)
        {
            yield return StartLine;
            for (var first = 0; first < rowCount; first += rowsPerLine)
                yield return DataLine(first, Math.Min(rowsPerLine, rowCount - first));
            yield return FinishLine;
        }

        private static IEnumerable<string> GatedLines(ManualResetEventSlim releaseRest)
        {
            yield return StartLine;
            yield return DataLine(0, 1);
            releaseRest.Wait();
            yield return DataLine(1, 3);
            yield return FinishLine;
        }

        private static IEnumerable<string> SlowLines(int lineCount, int delayMillis)
        {
            yield return StartLine;
            for (var id = 0; id < lineCount; id++)
            {
                Thread.Sleep(delayMillis);
                yield return DataLine(id, 1);
            }
            yield return FinishLine;
        }

        private static string DataLine(int firstId, int rowCount)
        {
            return JsonLinesMessages.Data(Enumerable.Range(firstId, rowCount).Select(id => new object?[] { id, Payload }));
        }

        /// <summary>
        /// A response body generated lazily line by line, so tests can stream far more data than they hold.
        /// </summary>
        private sealed class JsonLinesStream : Stream
        {
            private readonly IEnumerator<string> _lines;
            private readonly bool _stallAtEnd;
            private readonly System.Exception? _failAtEnd;
            private byte[] _current = Array.Empty<byte>();
            private int _offset;

            /// <param name="stallAtEnd">Instead of ending, wait like a server that stopped sending, until the read is cancelled.</param>
            /// <param name="failAtEnd">Instead of ending, fail like a broken connection.</param>
            public JsonLinesStream(IEnumerable<string> lines, bool stallAtEnd = false, System.Exception? failAtEnd = null)
            {
                _lines = lines.GetEnumerator();
                _stallAtEnd = stallAtEnd;
                _failAtEnd = failAtEnd;
            }

            public long BytesProduced { get; private set; }
            public bool IsDisposed { get; private set; }

            public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!NextBytesAvailable())
                {
                    if (_failAtEnd != null) throw _failAtEnd;
                    if (!_stallAtEnd) return 0;
                    // Like SocketsHttpHandler, a cancelled token is what ends a read waiting on the network.
                    await Task.Delay(Timeout.Infinite, cancellationToken);
                }
                return Copy(buffer.Span);
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                if (!NextBytesAvailable())
                {
                    if (_failAtEnd != null) throw _failAtEnd;
                    if (!_stallAtEnd) return 0;
                    throw new NotSupportedException("A stalled read can only be ended through ReadAsync's token");
                }
                return Copy(buffer.AsSpan(offset, count));
            }

            private bool NextBytesAvailable()
            {
                if (IsDisposed) throw new ObjectDisposedException(nameof(JsonLinesStream));
                while (_offset == _current.Length)
                {
                    if (!_lines.MoveNext()) return false;
                    _current = Encoding.UTF8.GetBytes(_lines.Current + "\n");
                    _offset = 0;
                }
                return true;
            }

            private int Copy(Span<byte> destination)
            {
                var n = Math.Min(destination.Length, _current.Length - _offset);
                _current.AsSpan(_offset, n).CopyTo(destination);
                _offset += n;
                BytesProduced += n;
                return n;
            }

            protected override void Dispose(bool disposing)
            {
                IsDisposed = true;
                base.Dispose(disposing);
            }

            public override bool CanRead => true;
            public override bool CanSeek => false;
            public override bool CanWrite => false;
            public override long Length => throw new NotSupportedException();
            public override long Position
            {
                get => throw new NotSupportedException();
                set => throw new NotSupportedException();
            }
            public override void Flush() { }
            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        }
    }
}
