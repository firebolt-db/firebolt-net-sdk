using System.Reflection;

namespace FireboltDotNetSdk;

public class HttpClientSingleton
{
    private static HttpClient? _instance;
    private static readonly object Mutex = new();

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
        var client = new HttpClient();

        // Disable timeouts
        client.Timeout = TimeSpan.FromMilliseconds(-1);

        var version = Assembly.GetEntryAssembly()?.GetName()?.Version?.ToString();
        client.DefaultRequestHeaders.Add("User-Agent", ".NETSDK/.NET6_" + version);
        return client;
    }
}