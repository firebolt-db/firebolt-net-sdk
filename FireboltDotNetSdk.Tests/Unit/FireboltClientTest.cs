using FireboltDotNetSdk.Exception;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    public class FireboltClientTest
    {

        [Test]
        public void ExecuteQueryAsyncNullDataTest()
        {
            try
            {
                FireboltClient client = new FireboltClient("test.api.firebolt.io", "any", "any");
                client.ExecuteQueryAsync("", "databaseName", "commandText", new HashSet<string>(), CancellationToken.None)
                    .GetAwaiter().GetResult();
            }
            catch (System.Exception e)
            {
                Assert.That(e.Message, Is.EqualTo("Some parameters are null or empty: engineEndpoint: , databaseName: databaseName or query: commandText"));
            }

        }
        [Test]
        public void GetAccountIdByNameAsyncTest()
        {
            FireboltClient client = new FireboltClient("test.api.firebolt.io", "any", "any");

            try
            {
                client.GetAccountIdByNameAsync(null, CancellationToken.None).GetAwaiter().GetResult();
            }
            catch (FireboltException e)
            {
                Assert.That(e.Message, Is.EqualTo("Account name is empty"));
            }
        }

        [Test]
        public void GetEngineUrlByDatabaseNameTest()
        {
            FireboltClient client = new FireboltClient("test.api.firebolt.io", "any", "any");
            var status = client.GetEngineUrlByDatabaseName("DBName", "AccountName");
            Assert.That(status.Status.ToString(), Is.EqualTo("Faulted"));
        }
        
        [Test]
        public void ExecuteQueryWithoutAccessTokenExceptionTest()
        {
            FireboltClient client = new FireboltClient("test.api.firebolt.io", "any", "any");
            try
            {
                client.ExecuteQuery("DBName", "EngineURL", "Select 1").GetAwaiter().GetResult();
                Assert.Fail("This query must throw an exception");
            }
            catch (FireboltException e)
            {
                Assert.That(e.Message, Is.EqualTo("The Access token is null or empty - EstablishConnection must be called"));
            }
            
        }

        [Test]
        public void ExecuteQueryExceptionTest()
        {
            FireboltClient client = new FireboltClient("test.api.firebolt.io", "any", "any");
            var tokenField = client.GetType().GetField("_loginToken", System.Reflection.BindingFlags.NonPublic
                                                                      | System.Reflection.BindingFlags.Instance);
            tokenField.SetValue(client, new FireboltClient.Token("abc",null, null));
            Assert.ThrowsAsync<HttpRequestException>(() => { client.ExecuteQuery("DBName", "EngineURL", "Select 1").GetAwaiter().GetResult(); return Task.CompletedTask; });
        }

        [Test]
        public void ExecuteQueryTest()
        {
            FireboltClient client = new FireboltClient("user", "password", "http://test.api.firebolt.io");
            var tokenField = client.GetType().GetField("_loginToken", System.Reflection.BindingFlags.NonPublic
                                                                | System.Reflection.BindingFlags.Instance);
            tokenField.SetValue(client, new FireboltClient.Token("abc",null, null));
            Assert.ThrowsAsync<HttpRequestException>(() => { client.ExecuteQuery("endpoint_url", "DBName", "Select 1").GetAwaiter().GetResult(); return Task.CompletedTask; });
        }
    }
    
}
