using FireboltDotNetSdk.Exception;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    public class FireboltClientTest
    {

        [Test]
        public void ExecuteQueryAsyncNullDataTest()
        {
            FireboltClient client = new FireboltClient("any", "any", endpoint: "test.api.firebolt.io");
            FireboltException? exception = Assert.Throws<FireboltException>(() => client.ExecuteQueryAsync("", "databaseName", null, "commandText", new HashSet<string>(), CancellationToken.None)
                    .GetAwaiter().GetResult());
            Assert.NotNull(exception);
            Assert.That(exception!.Message, Is.EqualTo("Some parameters are null or empty: engineEndpoint: , databaseName: databaseName or query: commandText"));
        }

        [Test]
        public void ExecuteQueryWithoutAccessTokenExceptionTest()
        {
            FireboltClient client = new FireboltClient("any", "any", endpoint: "test.api.firebolt.io");
            FireboltException? exception = Assert.Throws<FireboltException>(() => client.ExecuteQuery("DBName", "EngineURL", null, "Select 1").GetAwaiter().GetResult());
            Assert.NotNull(exception);
            Assert.That(exception!.Message, Is.EqualTo("The Access token is null or empty - EstablishConnection must be called"));
        }

        [Test]
        public void ExecuteQueryExceptionTest()
        {
            FireboltClient client = new FireboltClient("any", "any", endpoint: "test.api.firebolt.io");
            var tokenField = client.GetType().GetField("_token", System.Reflection.BindingFlags.NonPublic
                                                                      | System.Reflection.BindingFlags.Instance);
            Assert.NotNull(tokenField);
            tokenField!.SetValue(client, "abc");
            Assert.ThrowsAsync<HttpRequestException>(() => { client.ExecuteQuery("DBName", "EngineURL", null, "Select 1").GetAwaiter().GetResult(); return Task.CompletedTask; });
        }

        [Test]
        public void ExecuteQueryTest()
        {
            FireboltClient client = new FireboltClient("user", "password", endpoint: "http://test.api.firebolt.io");
            var tokenField = client.GetType().GetField("_token", System.Reflection.BindingFlags.NonPublic
                                                                | System.Reflection.BindingFlags.Instance);
            Assert.NotNull(tokenField);
            tokenField!.SetValue(client, "abc");
            Assert.ThrowsAsync<HttpRequestException>(() => { client.ExecuteQuery("endpoint_url", "DBName", null, "Select 1").GetAwaiter().GetResult(); return Task.CompletedTask; });
        }
    }

}
