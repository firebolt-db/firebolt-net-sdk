using System.Collections.Concurrent;
using static FireboltDotNetSdk.Client.FireResponse;

namespace FireboltDotNetSdk.Utils
{
    /// <summary>
    /// Caches JWT tokens, system engine URL, validated databases and engine options to avoid redundant authentication and 
    /// USE DATABASE/USE ENGINE queries when multiple connections are created with the same parameters.
    /// Aligns with JDBC ConnectionCache implementation.
    /// </summary>
    public class ConnectionCache
    {
        private readonly ConcurrentDictionary<string, bool> _validatedDatabases = new();
        private readonly ConcurrentDictionary<string, EngineOptions> _engineCache = new();
        private LoginResponse? _cachedToken;
        private string? _systemEngineUrl;
        private readonly object _tokenLock = new();

        /// <summary>
        /// Gets the connection ID that created this cache.
        /// </summary>
        public string ConnectionId { get; }

        /// <summary>
        /// Gets or sets the cache source (Memory or Disk).
        /// </summary>
        public string CacheSource { get; set; } = "Memory";

        /// <summary>
        /// Initializes a new instance of ConnectionCache.
        /// </summary>
        /// <param name="connectionId">The connection ID that creates this cache.</param>
        public ConnectionCache(string connectionId)
        {
            ConnectionId = connectionId;
        }

        /// <summary>
        /// Gets the cached JWT token if it hasn't expired.
        /// </summary>
        /// <returns>The cached login response, or null if not found or expired.</returns>
        public LoginResponse? GetCachedToken()
        {
            lock (_tokenLock)
            {
                if (_cachedToken != null && Convert.ToInt64(_cachedToken.Expires_in) < Constant.GetCurrentEpoch())
                {
                    // Token has expired
                    _cachedToken = null;
                }
                return _cachedToken;
            }
        }

        /// <summary>
        /// Caches the JWT token with its expiration time.
        /// </summary>
        /// <param name="tokenData">The login response containing the token with absolute expiration time already set.</param>
        public void SetCachedToken(LoginResponse tokenData)
        {
            lock (_tokenLock)
            {
                _cachedToken = tokenData;
            }
        }

        /// <summary>
        /// Gets the cached system engine URL.
        /// </summary>
        /// <returns>The cached system engine URL, or null if not found.</returns>
        public string? GetSystemEngineUrl()
        {
            return _systemEngineUrl;
        }

        /// <summary>
        /// Sets the system engine URL in the cache.
        /// </summary>
        /// <param name="systemEngineUrl">The system engine URL to cache.</param>
        public void SetSystemEngineUrl(string systemEngineUrl)
        {
            _systemEngineUrl = systemEngineUrl;
        }

        /// <summary>
        /// Checks if a database has been validated and cached.
        /// </summary>
        /// <param name="databaseName">The name of the database.</param>
        /// <returns>True if the database has been validated, false otherwise.</returns>
        public bool IsDatabaseValidated(string databaseName)
        {
            return _validatedDatabases.ContainsKey(databaseName);
        }

        /// <summary>
        /// Marks a database as validated in the cache.
        /// </summary>
        /// <param name="databaseName">The name of the database.</param>
        public void SetDatabaseValidated(string databaseName)
        {
            _validatedDatabases[databaseName] = true;
        }

        /// <summary>
        /// Gets the cached engine options for the specified engine name.
        /// </summary>
        /// <param name="engineName">The name of the engine.</param>
        /// <returns>The cached engine options, or null if not found.</returns>
        public EngineOptions? GetEngineOptions(string engineName)
        {
            _engineCache.TryGetValue(engineName, out var options);
            return options;
        }

        /// <summary>
        /// Sets the engine options in the cache.
        /// </summary>
        /// <param name="engineName">The name of the engine.</param>
        /// <param name="options">The engine options to cache.</param>
        public void SetEngineOptions(string engineName, EngineOptions options)
        {
            _engineCache[engineName] = options;
        }
    }
}
