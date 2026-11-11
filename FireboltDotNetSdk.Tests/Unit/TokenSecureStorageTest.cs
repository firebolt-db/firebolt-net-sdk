using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;
using Newtonsoft.Json;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    public class TokenSecureStorageTest
    {
        private string _path = "";

        [SetUp]
        public void SetUp()
        {
            _path = Path.Combine(Path.GetTempPath(), $"firebolt-token-{Guid.NewGuid():N}.json");
        }

        [TearDown]
        public void TearDown()
        {
            File.Delete(_path);
        }

        [Test]
        public async Task ReadsWhatCachingWrites()
        {
            await File.WriteAllTextAsync(_path, JsonConvert.SerializeObject(new CachedJSONData("token-value", "salt-value", 1700000000), Formatting.Indented));

            var data = await TokenSecureStorage.readDataJSONAsync(_path);

            Assert.Multiple(() =>
            {
                Assert.That(data!.token, Is.EqualTo("token-value"));
                Assert.That(data.salt, Is.EqualTo("salt-value"));
                Assert.That(data.expiration, Is.EqualTo(1700000000));
            });
        }

        [Test]
        public async Task IgnoresUnknownFields()
        {
            await File.WriteAllTextAsync(_path, "{\"token\":\"t\",\"salt\":\"s\",\"expiration\":5,\"other\":[1]}");

            var data = await TokenSecureStorage.readDataJSONAsync(_path);

            Assert.That((data!.token, data.salt, data.expiration), Is.EqualTo(("t", "s", 5L)));
        }

        [Test]
        public async Task MissingFileReturnsNull()
        {
            Assert.That(await TokenSecureStorage.readDataJSONAsync(_path), Is.Null);
        }

        [TestCase("")]
        [TestCase("not json")]
        [TestCase("{\"token\":\"t\"")]
        public void InvalidFileIsRejected(string content)
        {
            File.WriteAllText(_path, content);

            var exception = Assert.ThrowsAsync<FireboltException>(() => TokenSecureStorage.readDataJSONAsync(_path));
            Assert.That(exception!.Message, Is.EqualTo("Error while reading/deserializing JSON data "));
        }
    }
}
