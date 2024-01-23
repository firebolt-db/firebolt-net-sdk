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
        var httpHandler = new SocketsHttpHandler();
        httpHandler.KeepAlivePingDelay = TimeSpan.FromSeconds(KEEPALIVE_TIME);
        var client = new HttpClient(httpHandler);

        // Disable timeouts
        client.Timeout = TimeSpan.FromMilliseconds(-1);

        var version = Assembly.GetEntryAssembly()?.GetName()?.Version?.ToString();
        client.DefaultRequestHeaders.Add("User-Agent", ".NETSDK/.NET6_" + version);
        return client;
    }
}