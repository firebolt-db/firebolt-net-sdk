using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    public class FireboltClientTest
    {

        [Test]
        public void SetTokenTest()
        {
            var token = new FireResponse.LoginResponse()
            {
                Access_token = "randomNumber",
                Token_type = "JWT"
            };
            var client = new FireboltClient("test.api.firebolt.io");
            client.SetToken(token);
            Assert.That(client.Token.ToString(), Is.EqualTo("randomNumber"));
            Assert.That(token.Token_type, Is.EqualTo("JWT"));
        }

        [Test]
        public void ClearTokenTest()
        {
            var token = new FireResponse.LoginResponse()
            {
                Access_token = "randomNumber"
            };
            var client = new FireboltClient("test.api.firebolt.io");
            client.SetToken(token);
            client.ClearToken();
            Assert.IsNull(client.Token);
        }

        [Test]
        public void GetEngineUrlByDatabaseNameTest()
        {
            var client = new FireboltClient("test.api.firebolt.io");
            var status = client.GetEngineUrlByDatabaseName("DBName", "AccountName");
            Assert.That(status.Status.ToString(), Is.EqualTo("Faulted"));
        }

        [Test]
        public void ExecuteQueryExceptionTest()
        {
            var client = new FireboltClient("test.api.firebolt.io");
            try
            {
                var status = client.ExecuteQuery("DBName", "EngineURL", "Select 1").GetAwaiter().GetResult();
            }
            catch (System.Exception e)
            {
                Assert.That(e.Message, Is.EqualTo("Resource temporarily unavailable (dbname:443)"));
            }
        }

        [Test]
        public void ExecuteQueryTest()
        {
            var client = new FireboltClient("test.api.firebolt.io");
            try
            {
                var status = client.ExecuteQuery("endpoint_url", "DBName", "Select 1").GetAwaiter().GetResult();
            }
            catch (System.Exception e)
            {
                Assert.That(e.Message, Is.EqualTo("Resource temporarily unavailable (endpoint_url:443)"));
            }
        }
    }
}
