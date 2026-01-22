using System.Collections.Concurrent;

namespace FireboltDotNetSdk.Utils;

/// <summary>
///     Service for managing connection caches with automatic expiration.
///     Cache entries expire 1 hour after creation.
/// </summary>
public class CacheService
{
    private static readonly TimeSpan ExpirationDuration = TimeSpan.FromHours(1);
    private readonly ConcurrentDictionary<string, CacheEntry> _caches = new();

    private CacheService()
    {
    }

    public static CacheService Instance { get; } = new();

    /// <summary>
    ///     Gets the connection cache for the specified cache key value.
    ///     Returns null if not found or if the cache has expired.
    /// </summary>
    /// <param name="cacheKeyValue">The cache key value (hashed string).</param>
    /// <returns>The connection cache, or null if not found or expired.</returns>
    public ConnectionCache? Get(string cacheKeyValue)
    {
        if (!_caches.TryGetValue(cacheKeyValue, out var entry)) return null;
        if (!entry.IsExpired) return entry.Cache;
        // Remove expired entry
        _caches.TryRemove(cacheKeyValue, out _);
        return null;
    }

    /// <summary>
    ///     Puts a connection cache for the specified cache key value.
    ///     The cache will expire 1 hour after being stored.
    /// </summary>
    /// <param name="cacheKeyValue">The cache key value (hashed string).</param>
    /// <param name="cache">The connection cache to store.</param>
    public void Put(string cacheKeyValue, ConnectionCache cache)
    {
        _caches[cacheKeyValue] = new CacheEntry(cache);
    }

    /// <summary>
    ///     Gets or creates a connection cache for the specified cache key value.
    ///     If creating a new cache, it will expire 1 hour after creation.
    ///     If the existing cache is expired, it will be removed and a new one created.
    /// </summary>
    /// <param name="cacheKeyValue">The cache key value (hashed string).</param>
    /// <param name="connectionId">The connection ID to use if creating a new cache.</param>
    /// <returns>The connection cache.</returns>
    public ConnectionCache GetOrCreate(string cacheKeyValue, string connectionId)
    {
        // Try to get existing non-expired cache
        var existingCache = Get(cacheKeyValue);
        if (existingCache != null) return existingCache;

        // Create new entry
        var newEntry = new CacheEntry(new ConnectionCache(connectionId));
        return _caches.GetOrAdd(cacheKeyValue, newEntry).Cache;
    }

    /// <summary>
    ///     Clears all connection caches.
    /// </summary>
    public void Clear()
    {
        _caches.Clear();
    }

    private sealed class CacheEntry
    {
        public CacheEntry(ConnectionCache cache)
        {
            Cache = cache;
            ExpiresAt = DateTime.UtcNow.Add(ExpirationDuration);
        }

        public ConnectionCache Cache { get; }
        private DateTime ExpiresAt { get; }

        public bool IsExpired => DateTime.UtcNow >= ExpiresAt;
    }
}