using System.Net;
using System.Reflection;
using System.Net.Sockets;


namespace FireboltDotNetSdk;

public class HttpClientSingleton
{
    private static HttpClient? _instance;
    private static readonly object Mutex = new();
    private const int KEEPALIVE_TIME = 60;

    /// <summary>
    ///     Returns a shared instance of the Firebolt client.
    /// </summary>
    public static HttpClient GetInstance()
    {
        if (_instance == null)
            lock (Mutex)
            {
                _instance ??= CreateClient();
            }
        return _instance;
    }

    private static HttpClient CreateClient()
    {
        var httpHandler = new SocketsHttpHandler
        {
            ConnectCallback = (context, token) => ConfigureSocketTcpKeepAlive(context, token),
        };
        var client = new HttpClient(httpHandler);

        // Disable timeouts
        client.Timeout = TimeSpan.FromMilliseconds(-1);

        var version = Assembly.GetEntryAssembly()?.GetName()?.Version?.ToString();
        client.DefaultRequestHeaders.Add("User-Agent", ".NETSDK/.NET6_" + version);
        return client;
    }

    private static async ValueTask<Stream> ConfigureSocketTcpKeepAlive(
        SocketsHttpConnectionContext context,
        CancellationToken token)
    {
        Socket socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        try
        {
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, KEEPALIVE_TIME);

            // As a workaround for PlatformNotSupportedException, we use Dns.GetHostAddressesAsync to resolve and 
            // pass the IP address to Socket.ConnectAsync. 
            // see: https://github.com/dotnet/runtime/issues/24917 
            var address = await Dns.GetHostAddressesAsync(context.DnsEndPoint.Host).ConfigureAwait(false);
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