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
}