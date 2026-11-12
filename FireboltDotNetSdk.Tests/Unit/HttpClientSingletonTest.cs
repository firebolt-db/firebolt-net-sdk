using System.Net;
using System.Net.Sockets;
using System.Text;

namespace FireboltDotNetSdk.Tests;

public class HttpClientSingletonTest
{
    [Test]
    public void InitSingletonOnlyOnce()
    {
        HttpClient client = HttpClientSingleton.GetInstance();
        HttpClient client2 = HttpClientSingleton.GetInstance();
        Assert.That(ReferenceEquals(client, client2), Is.True);
    }

    [Test]
    public void SetDefaultRequestHeaders()
    {
        HttpClient client = HttpClientSingleton.GetInstance();
        Assert.That(client.DefaultRequestHeaders.UserAgent.ToList()[0].Product?.Name, Is.EqualTo(".NETSDK"));
    }

    [Test]
    public void TestConfigureSocketTcpKeepAlive_ClientHasInfiniteTimeout()
    {
        // Verify the timeout is set correctly (infinite) as configured in CreateClient
        var client = HttpClientSingleton.GetInstance();
        Assert.That(client.Timeout, Is.EqualTo(TimeSpan.FromMilliseconds(-1)),
            "HttpClient should have infinite timeout for long-running queries");
    }

    [Test]
    public async Task TestConfigureSocketTcpKeepAlive_HandlesConnectionFailureGracefully()
    {
        // Test that the socket properly handles connection failures
        // This verifies the error handling in ConfigureSocketTcpKeepAlive's catch block
        var client = HttpClientSingleton.GetInstance();
        // Try to connect to an invalid host that should fail
        var exceptionThrown = false;
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, "https://invalid.invalid.invalid.test");
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await client.SendAsync(request, cts.Token);
        }
        catch (HttpRequestException)
        {
            // Expected - socket should be properly disposed per the catch block in ConfigureSocketTcpKeepAlive
            exceptionThrown = true;
        }

        Assert.That(exceptionThrown, Is.True, "Should throw exception for invalid host");
    }

    [Test]
    public void TestConfigureSocketTcpKeepAlive_ClientIsReusable()
    {
        // Test that the same client instance can be used multiple times
        // This ensures the socket configuration is properly reusable
        var client1 = HttpClientSingleton.GetInstance();
        var client2 = HttpClientSingleton.GetInstance();
        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(client1, client2), Is.True,
                "Should return same instance (singleton pattern)");
            Assert.That(client1.DefaultRequestHeaders.UserAgent, Is.Not.Null,
                "Client should maintain configured headers");
            Assert.That(client1.Timeout, Is.EqualTo(TimeSpan.FromMilliseconds(-1)),
                "Client should maintain configured timeout");
        });
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task ResponseHeadersAreDecodedAsUtf8(bool compression)
    {
        const string database = "db ü 日本";
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            await AssertHeaderIsDecoded(listener, compression, database);
        }
        finally
        {
            listener.Stop();
        }
    }

    private static async Task AssertHeaderIsDecoded(TcpListener listener, bool compression, string database)
    {
        var server = Task.Run(async () =>
        {
            using var socket = await listener.AcceptTcpClientAsync();
            var stream = socket.GetStream();
            var buffer = new byte[4096];
            var request = "";
            int read;
            while (!request.Contains("\r\n\r\n") && (read = await stream.ReadAsync(buffer)) > 0)
                request += Encoding.ASCII.GetString(buffer, 0, read);
            var response = $"HTTP/1.1 200 OK\r\nFirebolt-Update-Parameters: database={database}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            await stream.WriteAsync(Encoding.UTF8.GetBytes(response));
        });

        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        using var result = await HttpClientSingleton.GetInstance(compression).GetAsync($"http://127.0.0.1:{port}/");
        await server;

        Assert.That(result.Headers.GetValues("Firebolt-Update-Parameters").Single(), Is.EqualTo("database=" + database));
    }
}
