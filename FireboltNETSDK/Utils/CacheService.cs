using System.Collections.Concurrent;

namespace FireboltDotNetSdk.Utils
{
    /// <summary>
    /// Service for managing connection caches.
    /// </summary>
    public class CacheService
    {
        private static readonly CacheService _instance = new();
        private readonly ConcurrentDictionary<string, ConnectionCache> _caches = new();

        private CacheService()
        {
        }

        public static CacheService Instance => _instance;

        /// <summary>
        /// Gets the connection cache for the specified cache key value.
        /// </summary>
        /// <param name="cacheKeyValue">The cache key value (hashed string).</param>
        /// <returns>The connection cache, or null if not found.</returns>
        public ConnectionCache? Get(string cacheKeyValue)
        {
            _caches.TryGetValue(cacheKeyValue, out var cache);
            return cache;
        }

        /// <summary>
        /// Puts a connection cache for the specified cache key value.
        /// </summary>
        /// <param name="cacheKeyValue">The cache key value (hashed string).</param>
        /// <param name="cache">The connection cache to store.</param>
        public void Put(string cacheKeyValue, ConnectionCache cache)
        {
            _caches[cacheKeyValue] = cache;
        }

        /// <summary>
        /// Gets or creates a connection cache for the specified cache key value.
        /// </summary>
        /// <param name="cacheKeyValue">The cache key value (hashed string).</param>
        /// <param name="connectionId">The connection ID to use if creating a new cache.</param>
        /// <returns>The connection cache.</returns>
        public ConnectionCache GetOrCreate(string cacheKeyValue, string connectionId)
        {
            return _caches.GetOrAdd(cacheKeyValue, _ => new ConnectionCache(connectionId));
        }

        /// <summary>
        /// Clears all connection caches.
        /// </summary>
        public void Clear()
        {
            _caches.Clear();
        }
    }
}
