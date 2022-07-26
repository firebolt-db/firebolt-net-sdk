using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using Newtonsoft.Json;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    public class FireboltCommandTest
    {
        private FireboltCommand _fireboltCommand;
        private HttpClient _client;

        [SetUp]
        public void Init()
        {
            _client = new HttpClient();
            _fireboltCommand = new FireboltCommand("commandText");
            _fireboltCommand.Response =
                "{\"query\":{\"query_id\": \"16FDB86662938757\"},\"meta\":[{\"name\": \"uint8\",\"type\": \"UInt8\"}],\"data\":[[1]],\"rows\": 1,\"statistics\":{\"elapsed\": 0.000620069,\"rows_read\": 1,\"bytes_read\": 1,\"time_before_execution\": 0.000409657,\"time_to_execute\": 0.000208377,\"scanned_bytes_cache\": 0,\"scanned_bytes_storage\": 0}}";
        }

        [TestCase("SET param=1")]
        [TestCase("SET param=1,param=2")]
        public void ExecuteTest(string commandText)
        {
            var cs = new FireboltCommand(commandText);
            cs.Execute(commandText);
            Assert.IsNotEmpty(FireboltCommand.SetParamList);
        }

        [TestCase("Select 1")]
        public void ExecuteWrongWaySelectTest(string commandText)
        {
            var cs = new FireboltCommand(commandText);
            try
            {
                cs.Execute(commandText);
            }
            catch (FireboltException ex)
            {
                Assert.That(ex.Message, Is.EqualTo("JSON data is missing"));
            }
        }

        [Test]
        public void GetOriginalJsonDataExceptionTest()
        {
            var cs = new FireboltCommand("commandText");
            try
            {
                cs.GetOriginalJsonData();
            }
            catch (FireboltException ex)
            {
                Assert.That(ex.Message, Is.EqualTo("Response is empty while GetOriginalJSONData"));
            }
        }

        [Test]
        public void GetOriginalJsonDataTest()
        {
            var result = _fireboltCommand.GetOriginalJsonData();
            Assert.IsNotNull(result.Data.Count);
        }

        [Test]
        public void RowCountTest()
        {
            var result = _fireboltCommand.RowCount();
            Assert.IsNotNull(result);
        }

        [Test]
        public void RowCountExcecptionTest()
        {
            var cs = new FireboltCommand("commandText");
            try
            {
                cs.RowCount();
            }
            catch (FireboltException ex)
            {
                Assert.That(ex.Message, Is.EqualTo("RowCount is missing"));
            }
        }

        [TestCase("SET param=1")]
        [TestCase("SET param=1,param=2")]
        public void ClearSetListTest(string commandText)
        {
            var cs = new FireboltCommand(commandText);
            cs.Execute(commandText);
            Assert.IsNotEmpty(FireboltCommand.SetParamList);
            cs.ClearSetList();
            Assert.IsEmpty(FireboltCommand.SetParamList);
        }


        [Test]
        public void PrepareRequestTest()
        {
            _client = new HttpClient();
            var cs = new FireboltCommand("commandText");
            cs.PrepareRequest(_client);
            Assert.AreEqual(_client.DefaultRequestHeaders.UserAgent.ToList()[0].Product.Name, "Other");
        }

        [Test]
        public void PrepareRequestTokenTest()
        {
            _client = new HttpClient();
            var cs = new FireboltCommand("commandText");
            cs.PrepareRequest(_client);
            Assert.AreEqual(_client.DefaultRequestHeaders.UserAgent.ToList()[0].Product.Name, "Other");
            cs.Token = "notNull";
            cs.PrepareRequest(_client);
            Assert.AreEqual(_client.DefaultRequestHeaders.Authorization.Parameter, "notNull");
        }

        [Test]
        public void ExecuteQueryAsyncNullDataTest()
        {
            try
            {
                var task = _fireboltCommand.ExecuteQueryAsync("", "databaseName", "commandText", CancellationToken.None)
                    .GetAwaiter().GetResult();
            }
            catch (System.Exception e)
            {
                Assert.AreEqual(e.Message,
                    "Something parameters are null or empty: engineEndpoint: , databaseName: databaseName or query: commandText");
            }

        }

        [Test]
        public void CreateSerializerSettingsTest()
        {
            var settings = new JsonSerializerSettings();
            var result = FireboltCommand.CreateSerializerSettings();
            Assert.That(result.GetType(), Is.EqualTo(settings.GetType()));
        }

        [Test]
        public void AuthV1LoginAsyncTest()
        {
            var cs = new FireboltCommand("https://api.dev.firebolt.io");
            var creds = new FireRequest.LoginRequest()
            {
                Username = "userName",
                Password = "passWord"
            };
            try
            {
                cs.AuthV1LoginAsync(creds).GetAwaiter().GetResult();
            }
            catch (System.Exception e)
            {
                Assert.IsTrue(e.Message.Contains("429"));
            }
        }

        [Test]
        public void GetAccountIdByNameAsyncTest()
        {
            var cs = new FireboltCommand("https://api.dev.firebolt.io");
            
            try
            {
                cs.GetAccountIdByNameAsync(null,CancellationToken.None).GetAwaiter().GetResult();
            }
            catch (FireboltException e)
            {
                Assert.AreEqual(e.Message, "Account name is empty");
            }
        }

        [Test]
        public void FormDataForResponseTest()
        {
            var result = FireboltCommand.FormDataForResponse(_fireboltCommand.Response);
            Assert.IsNotNull(result);
        }

        [Test]
        public void FormDataForResponseInvalidTest()
        {
            try
            {
                FireboltCommand.FormDataForResponse(null);
            }
            catch (FireboltException e)
            {
                Assert.AreEqual(e.Message, "JSON data is missing");
            }
        }
    }
}
