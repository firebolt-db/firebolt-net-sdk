using FireboltDotNetSdk.Utils;
using static FireboltDotNetSdk.Client.FireResponse;

namespace FireboltDotNetSdk.Tests.Unit
{
    [TestFixture]
    public class ConnectionCacheTest
    {
        [Test]
        public void TestConnectionCache_JwtTokenCaching()
        {
            var connectionId = Guid.NewGuid().ToString();
            var cache = new ConnectionCache(connectionId);
            
            // Initially no cached token
            Assert.That(cache.GetCachedToken(), Is.Null);
            
            // Set a token with future expiration
            var tokenData = new LoginResponse("test_token", "3600", "Bearer");
            
            cache.SetCachedToken(tokenData);
            
            // Should retrieve the cached token
            var retrievedToken = cache.GetCachedToken();
            Assert.That(retrievedToken, Is.Not.Null);
            Assert.That(retrievedToken!.Access_token, Is.EqualTo("test_token"));
        }

        [Test]
        public void TestConnectionCache_JwtTokenExpiration()
        {
            var connectionId = Guid.NewGuid().ToString();
            var cache = new ConnectionCache(connectionId);
            
            // Set a token that has already expired
            // Pass -3600 so that when SetCachedToken adds it to current epoch, it results in a past time
            var expiredToken = new LoginResponse("expired_token", "-3600", "Bearer");
            
            cache.SetCachedToken(expiredToken);
            
            // Should return null for expired token
            var retrievedToken = cache.GetCachedToken();
            Assert.That(retrievedToken, Is.Null);
        }

        [Test]
        public void TestConnectionCache_SystemEngineUrl()
        {
            var connectionId = Guid.NewGuid().ToString();
            var cache = new ConnectionCache(connectionId);
            
            // Initially no system engine URL
            Assert.That(cache.GetSystemEngineUrl(), Is.Null);
            
            // Set system engine URL
            cache.SetSystemEngineUrl("https://api.example.com/engine");
            
            // Should retrieve the URL
            Assert.That(cache.GetSystemEngineUrl(), Is.EqualTo("https://api.example.com/engine"));
        }

        [Test]
        public void TestConnectionCache_DatabaseValidation()
        {
            var connectionId = Guid.NewGuid().ToString();
            var cache = new ConnectionCache(connectionId);
            Assert.Multiple(() =>
            {

                // Initially database is not validated
                Assert.That(cache.IsDatabaseValidated("test_db1"), Is.False);
                Assert.That(cache.IsDatabaseValidated("test_db2"), Is.False);
            });

            // Mark database as validated
            cache.SetDatabaseValidated("test_db1");
            cache.SetDatabaseValidated("test_db2");
            Assert.Multiple(() =>
            {

                // Should now be validated
                Assert.That(cache.IsDatabaseValidated("test_db1"), Is.True);
                Assert.That(cache.IsDatabaseValidated("test_db2"), Is.True);

                // Different database should not be validated
                Assert.That(cache.IsDatabaseValidated("other_db"), Is.False);
            });
        }

        [Test]
        public void TestConnectionCache_EngineOptions()
        {
            var connectionId = Guid.NewGuid().ToString();
            var cache = new ConnectionCache(connectionId);
            
            // Initially no engine options
            Assert.That(cache.GetEngineOptions("test_engine"), Is.Null);
            
            // Set engine options
            var engineOptions = new EngineOptions(
                "https://engine.example.com",
                new List<KeyValuePair<string, string>>
                {
                    new("engine", "test_engine"),
                    new("param2", "value2")
                }
            );
            
            cache.SetEngineOptions("test_engine", engineOptions);
            
            // Should retrieve engine options
            var retrieved = cache.GetEngineOptions("test_engine");
            Assert.That(retrieved, Is.Not.Null);
            Assert.Multiple(() =>
            {
                Assert.That(retrieved.EngineUrl, Is.EqualTo("https://engine.example.com"));
                Assert.That(retrieved.Parameters, Has.Count.EqualTo(2));
            });
            Assert.Multiple(() =>
            {
                Assert.That(retrieved.Parameters[0].Key, Is.EqualTo("engine"));
                Assert.That(retrieved.Parameters[0].Value, Is.EqualTo("test_engine"));
                Assert.That(retrieved.Parameters[1].Key, Is.EqualTo("param2"));
                Assert.That(retrieved.Parameters[1].Value, Is.EqualTo("value2"));
            });
        }

        [Test]
        public void TestConnectionCache_MultipleEngineOptions()
        {
            var connectionId = Guid.NewGuid().ToString();
            var cache = new ConnectionCache(connectionId);
            
            // Set options for multiple engines
            var engine1Options = new EngineOptions(
                "https://engine1.example.com",
                new List<KeyValuePair<string, string>>
                {
                    new("engine", "engine1")
                }
            );
            
            var engine2Options = new EngineOptions(
                "https://engine2.example.com",
                new List<KeyValuePair<string, string>>
                {
                    new("engine", "engine2")
                }
            );
            
            cache.SetEngineOptions("engine1", engine1Options);
            cache.SetEngineOptions("engine2", engine2Options);
            
            // Should retrieve correct options for each engine
            var retrieved1 = cache.GetEngineOptions("engine1");
            var retrieved2 = cache.GetEngineOptions("engine2");
            
            Assert.That(retrieved1!.EngineUrl, Is.EqualTo("https://engine1.example.com"));
            Assert.That(retrieved2!.EngineUrl, Is.EqualTo("https://engine2.example.com"));
        }

        [Test]
        public void TestEngineOptions_NullUrlThrowsException()
        {
            var parameters = new List<KeyValuePair<string, string>>();
            
            Assert.Throws<ArgumentNullException>(() => new EngineOptions(null!, parameters));
        }
    }
}
