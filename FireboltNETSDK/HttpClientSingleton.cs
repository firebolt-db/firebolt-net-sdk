using System.Net;
using System.Reflection;
using System.Net.Sockets;


namespace FireboltDotNetSdk;

public static class HttpClientSingleton
{
    private static HttpClient? _instance;
    private static HttpClient? _unsafeInstance;
    private static readonly object Mutex = new();
    private static readonly object UnsafeMutex = new();
    private const int KEEPALIVE_TIME = 60;

    /// <summary>
    ///     Returns a shared instance of the Firebolt client.
    /// </summary>
    public static HttpClient GetInstance()
    {
        if (_instance != null) return _instance;
        lock (Mutex)
        {
            _instance ??= CreateClient();
        }
        return _instance;
    }

    /// <summary>
    ///     Returns a shared instance that does not validate TLS certificates.
    /// </summary>
    public static HttpClient GetUnsafeInstance()
    {
        if (_unsafeInstance != null) return _unsafeInstance;
        lock (UnsafeMutex)
        {
            _unsafeInstance ??= CreateClient(validateServerCertificate: false);
        }
        return _unsafeInstance;
    }

    private static HttpClient CreateClient(bool validateServerCertificate = true)
    {
        var httpHandler = new SocketsHttpHandler
        {
            ConnectCallback = ConfigureSocketTcpKeepAlive,
        };
        if (!validateServerCertificate)
        {
            httpHandler.SslOptions.RemoteCertificateValidationCallback = (_, _, _, _) => true;
        }
        var client = new HttpClient(httpHandler);

        // Disable timeouts
        client.Timeout = TimeSpan.FromMilliseconds(-1);

        var version = typeof(HttpClientSingleton).Assembly?.GetName().Version?.ToString();
        client.DefaultRequestHeaders.Add("User-Agent", ".NETSDK/.NET6_" + version);
        return client;
    }

    private static async ValueTask<Stream> ConfigureSocketTcpKeepAlive(
        SocketsHttpConnectionContext context,
        CancellationToken token)
    {
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        try
        {
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, KEEPALIVE_TIME);

            // As a workaround for PlatformNotSupportedException, we use Dns.GetHostAddressesAsync to resolve and 
            // pass the IP address to Socket.ConnectAsync. 
            // see: https://github.com/dotnet/runtime/issues/24917 
            var address = await Dns.GetHostAddressesAsync(context.DnsEndPoint.Host, token).ConfigureAwait(false);
            await socket.ConnectAsync(address, context.DnsEndPoint.Port, token).ConfigureAwait(false);

            return new NetworkStream(socket, ownsSocket: true);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }
}