using FireboltDotNetSdk.Exception;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    public class FireboltClientTest
    {

        [Test]
        public void InitSingletonOnlyOnce()
        {
            FireboltClient fireboltClient = FireboltClient.GetInstance();
            FireboltClient fireboltClient2 = FireboltClient.GetInstance();
            Assert.True(ReferenceEquals(fireboltClient, fireboltClient2));
        }
        
        [Test]
        public void SetDefaultRequestHeaders()
        {
            FireboltClient client = FireboltClient.GetInstance();
            Assert.That(client.HttpClient.DefaultRequestHeaders.UserAgent.ToList()[0].Product.Name, Is.EqualTo(".NETSDK"));
        }
        
        [Test]
        public void ExecuteQueryAsyncNullDataTest()
        {
            try
            {
                FireboltClient client = FireboltClient.GetInstance();
                client.ExecuteQueryAsync("", "databaseName", "commandText", CancellationToken.None, new HashSet<string>(), "")
                    .GetAwaiter().GetResult();
            }
            catch (System.Exception e)
            {
                Assert.That(e.Message, Is.EqualTo("Something parameters are null or empty: engineEndpoint: , databaseName: databaseName or query: commandText"));
            }
        
        }
        [Test]
        public void GetAccountIdByNameAsyncTest()
        {
            FireboltClient client = FireboltClient.GetInstance();
        
            try
            {
                client.GetAccountIdByNameAsync(null, CancellationToken.None, "base.url", "accessToken").GetAwaiter().GetResult();
            }
            catch (FireboltException e)
            {
                Assert.That(e.Message, Is.EqualTo("Account name is empty"));
            }
        }
        
        [Test]
        public void GetEngineUrlByDatabaseNameTest()
        {
            var client = FireboltClient.GetInstance();
            var status = client.GetEngineUrlByDatabaseName("DBName", "AccountName", "test.api.firebolt.io", "aToken");
            Assert.That(status.Status.ToString(), Is.EqualTo("Faulted"));
        }

        [Test]
        public void ExecuteQueryExceptionTest()
        {
            var client = FireboltClient.GetInstance();
            Assert.ThrowsAsync<HttpRequestException>(() => { client.ExecuteQuery("DBName", "EngineURL", "Select 1","aToken").GetAwaiter().GetResult(); return Task.CompletedTask; });
        }

        [Test]
        public void ExecuteQueryTest()
        {
            var client = FireboltClient.GetInstance();
            Assert.ThrowsAsync<HttpRequestException>(() => { client.ExecuteQuery("endpoint_url", "DBName", "Select 1", "aToken").GetAwaiter().GetResult(); return Task.CompletedTask; });
        }
        

        
    }
}
