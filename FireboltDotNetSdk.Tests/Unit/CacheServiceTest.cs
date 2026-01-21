using System.Collections.Concurrent;
using System.Reflection;
using FireboltDotNetSdk.Utils;

namespace FireboltDotNetSdk.Tests.Unit;

[TestFixture]
public class CacheServiceTest
{
    [SetUp]
    public void SetUp()
    {
        // Clear cache before each test
        CacheService.Instance.Clear();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        // Clean up after finishing tests
        CacheService.Instance.Clear();
    }

    [Test]
    public void TestCacheService_Singleton()
    {
        var instance1 = CacheService.Instance;
        var instance2 = CacheService.Instance;

        Assert.That(instance1, Is.SameAs(instance2));
    }

    [Test]
    public void TestCacheService_PutAndGet()
    {
        var cacheKey = new CacheKey("client_id", "client_secret", "account");
        var connectionId = Guid.NewGuid().ToString();
        var cache = new ConnectionCache(connectionId);

        // Put cache
        CacheService.Instance.Put(cacheKey.GetValue(), cache);

        // Get cache
        var retrieved = CacheService.Instance.Get(cacheKey.GetValue());

        Assert.That(retrieved, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(retrieved.ConnectionId, Is.EqualTo(connectionId));
            Assert.That(retrieved, Is.EqualTo(cache));
        });
    }

    [Test]
    public void TestCacheService_GetNonExistent()
    {
        var cacheKey = new CacheKey("nonexistent", "secret", "account");

        var retrieved = CacheService.Instance.Get(cacheKey.GetValue());

        Assert.That(retrieved, Is.Null);
    }

    [Test]
    public void TestCacheService_GetOrCreate()
    {
        var cacheKey = new CacheKey("client_id", "client_secret", "account");
        var connectionId = Guid.NewGuid().ToString();

        // First call creates new cache
        var cache1 = CacheService.Instance.GetOrCreate(cacheKey.GetValue(), connectionId);

        Assert.That(cache1, Is.Not.Null);
        Assert.That(cache1.ConnectionId, Is.EqualTo(connectionId));

        // Second call returns existing cache (with original connectionId)
        var newConnectionId = Guid.NewGuid().ToString();
        var cache2 = CacheService.Instance.GetOrCreate(cacheKey.GetValue(), newConnectionId);

        Assert.That(cache2, Is.SameAs(cache1));
        Assert.That(cache2.ConnectionId, Is.EqualTo(connectionId),
            "Should return original cache with original connectionId");
    }

    [Test]
    public void TestCacheService_Clear()
    {
        var cacheKey1 = new CacheKey("id1", "secret1", "account1");
        var cacheKey2 = new CacheKey("id2", "secret2", "account2");

        var cache1 = new ConnectionCache(Guid.NewGuid().ToString());
        var cache2 = new ConnectionCache(Guid.NewGuid().ToString());

        CacheService.Instance.Put(cacheKey1.GetValue(), cache1);
        CacheService.Instance.Put(cacheKey2.GetValue(), cache2);

        // Clear all caches
        CacheService.Instance.Clear();
        Assert.Multiple(() =>
        {
            // All should be cleared
            Assert.That(CacheService.Instance.Get(cacheKey1.GetValue()), Is.Null);
            Assert.That(CacheService.Instance.Get(cacheKey2.GetValue()), Is.Null);
        });
    }

    [Test]
    public void TestCacheService_IsolationBetweenAccounts()
    {
        var cacheKey1 = new CacheKey("client_id", "client_secret", "account1");
        var cacheKey2 = new CacheKey("client_id", "client_secret", "account2");

        var connectionId1 = Guid.NewGuid().ToString();
        var connectionId2 = Guid.NewGuid().ToString();

        var cache1 = new ConnectionCache(connectionId1);
        var cache2 = new ConnectionCache(connectionId2);

        cache1.SetDatabaseValidated("db1");
        cache2.SetDatabaseValidated("db2");

        CacheService.Instance.Put(cacheKey1.GetValue(), cache1);
        CacheService.Instance.Put(cacheKey2.GetValue(), cache2);

        // Caches should be isolated
        var retrieved1 = CacheService.Instance.Get(cacheKey1.GetValue());
        var retrieved2 = CacheService.Instance.Get(cacheKey2.GetValue());
        Assert.Multiple(() =>
        {
            Assert.That(retrieved1, Is.Not.Null);
            Assert.That(retrieved2, Is.Not.Null);
        });
        Assert.That(retrieved1, Is.Not.SameAs(retrieved2));
        Assert.Multiple(() =>
        {
            Assert.That(retrieved1!.IsDatabaseValidated("db1"), Is.True);
            Assert.That(retrieved1.IsDatabaseValidated("db2"), Is.False);
            Assert.That(retrieved2!.IsDatabaseValidated("db2"), Is.True);
            Assert.That(retrieved2.IsDatabaseValidated("db1"), Is.False);
        });
    }

    [Test]
    public void TestCacheService_ConcurrentPutAndGet()
    {
        var tasks = new List<Task>();
        var cacheKeys = new List<CacheKey>();

        // Create multiple cache entries concurrently
        for (var i = 0; i < 10; i++)
        {
            var cacheKey = new CacheKey($"id{i}", $"secret{i}", $"account{i}");
            cacheKeys.Add(cacheKey);

            tasks.Add(Task.Run(() =>
            {
                var connectionId = Guid.NewGuid().ToString();
                var cache = new ConnectionCache(connectionId);
                CacheService.Instance.Put(cacheKey.GetValue(), cache);
            }));
        }

        Task.WaitAll(tasks.ToArray());

        // Verify all caches are accessible
        foreach (var cache in cacheKeys.Select(cacheKey => CacheService.Instance.Get(cacheKey.GetValue())))
            Assert.That(cache, Is.Not.Null);
    }

    [Test]
    public void TestCacheService_GetOrCreate_ReusesNonExpiredCache()
    {
        var cacheKey = new CacheKey("client_id", "client_secret", "account");
        var connectionId1 = Guid.NewGuid().ToString();
        var connectionId2 = Guid.NewGuid().ToString();

        // Create initial cache
        var cache1 = CacheService.Instance.GetOrCreate(cacheKey.GetValue(), connectionId1);
        cache1.SetDatabaseValidated("test_db");

        // Second GetOrCreate should return the same cache instance
        var cache2 = CacheService.Instance.GetOrCreate(cacheKey.GetValue(), connectionId2);

        Assert.Multiple(() =>
        {
            Assert.That(cache2, Is.SameAs(cache1), "Should return the same cache instance");
            Assert.That(cache2.ConnectionId, Is.EqualTo(connectionId1), "Should have the original connection ID");
            Assert.That(cache2.IsDatabaseValidated("test_db"), Is.True, "Should have the previously cached data");
        });
    }

    [Test]
    public void TestCacheService_GetOrCreate_CreatesIfCacheExpired()
    {
        var cacheKey = new CacheKey("expired_client", "secret", "account");
        var oldConnectionId = Guid.NewGuid().ToString();
        var newConnectionId = Guid.NewGuid().ToString();

        // Use reflection to access private _caches field and CacheEntry type
        var cacheServiceType = typeof(CacheService);
        var cachesField = cacheServiceType.GetField("_caches", BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.That(cachesField, Is.Not.Null, "Should be able to access _caches field");

        var cachesDict = cachesField!.GetValue(CacheService.Instance);
        Assert.That(cachesDict, Is.Not.Null, "Should get _caches dictionary");

        // Get the CacheEntry type
        var cacheEntryType = cacheServiceType.GetNestedType("CacheEntry", BindingFlags.NonPublic);
        Assert.That(cacheEntryType, Is.Not.Null, "Should be able to access CacheEntry type");

        // Create a ConnectionCache with some data
        var oldConnectionCache = new ConnectionCache(oldConnectionId);
        oldConnectionCache.SetDatabaseValidated("old_db");

        // Create CacheEntry using reflection
        var cacheEntry = Activator.CreateInstance(cacheEntryType!, oldConnectionCache);
        Assert.That(cacheEntry, Is.Not.Null, "Should create CacheEntry instance");

        // Set ExpiresAt to a past date using reflection
        var expiresAtField = cacheEntryType!.GetField("<ExpiresAt>k__BackingField", BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.That(expiresAtField, Is.Not.Null, "Should be able to access ExpiresAt field");
        expiresAtField!.SetValue(cacheEntry, DateTime.UtcNow.AddHours(-1)); // Expired 1 hour ago

        // Add expired entry to cache dictionary
        var addMethod = cachesDict!.GetType().GetMethod("TryAdd");
        addMethod!.Invoke(cachesDict, new[] { cacheKey.GetValue(), cacheEntry });

        // Test 1: Get should return null for expired cache
        var retrieved = CacheService.Instance.Get(cacheKey.GetValue());
        Assert.That(retrieved, Is.Null, "Get should return null for expired cache");

        // Re-add expired entry for second test
        oldConnectionCache = new ConnectionCache(oldConnectionId);
        oldConnectionCache.SetDatabaseValidated("old_db");
        cacheEntry = Activator.CreateInstance(cacheEntryType, oldConnectionCache);
        expiresAtField.SetValue(cacheEntry, DateTime.UtcNow.AddHours(-1));
        addMethod.Invoke(cachesDict, new[] { cacheKey.GetValue(), cacheEntry });

        // Test 2: GetOrCreate should create a new cache when the old one is expired
        var newCache = CacheService.Instance.GetOrCreate(cacheKey.GetValue(), newConnectionId);

        Assert.Multiple(() =>
        {
            Assert.That(newCache, Is.Not.Null, "Should create new cache");
            Assert.That(newCache.ConnectionId, Is.EqualTo(newConnectionId), 
                "Should have new connection ID, not the expired one");
            Assert.That(newCache.IsDatabaseValidated("old_db"), Is.False, 
                "New cache should not have data from expired cache");
        });
    }
}