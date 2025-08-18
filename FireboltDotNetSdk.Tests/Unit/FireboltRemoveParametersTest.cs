using System.Net;
using System.Reflection;
using System.Text;
using FireboltDotNetSdk.Client;
using Moq;
using Moq.Protected;
using static Newtonsoft.Json.JsonConvert;

namespace FireboltDotNetSdk.Tests.Unit
{
    [TestFixture]
    public class FireboltRemoveParametersTest
    {
        private const string ConnectionString = "database=testdb.ib;clientid=testuser;clientsecret=test_pwd;account=accountname;endpoint=api.mock.firebolt.io";

        [Test]
        public async Task ProcessRemoveParametersHeader_SingleParameter_RemovesFromQueryParameters()
        {
            var (handlerMock, httpClient) = GetHttpMocks();
            var connection = new FireboltConnection(ConnectionString);
            var client = new FireboltClient2(connection, Guid.NewGuid().ToString(), "password", "http://test.api.firebolt.io", null, "account", httpClient);

            var tokenField = typeof(FireboltClient).GetField("_token", BindingFlags.NonPublic | BindingFlags.Instance);
            tokenField!.SetValue(client, "test_token");

            var queryParamsField = typeof(FireboltClient).GetField("_queryParameters", BindingFlags.NonPublic | BindingFlags.Instance);
            var queryParams = (IDictionary<string, string>)queryParamsField!.GetValue(client)!;
            queryParams["test_param"] = "test_value";
            queryParams["other_param"] = "other_value";

            var response = GetResponseMessage("test_result", HttpStatusCode.OK);
            response.Headers.Add("Firebolt-Remove-Parameters", "test_param");

            handlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(response);

            await client.ExecuteQueryAsync<string>("endpoint_url", "DBName", "account", "SELECT 1", new HashSet<string>(), CancellationToken.None);
            Assert.Multiple(() =>
            {
                Assert.That(queryParams.ContainsKey("test_param"), Is.False, "test_param should have been removed");
                Assert.That(queryParams.ContainsKey("other_param"), Is.True, "other_param should remain");
                Assert.That(queryParams["other_param"], Is.EqualTo("other_value"));
            });
        }

        [Test]
        public async Task ProcessRemoveParametersHeader_MultipleParameters_RemovesAllFromQueryParameters()
        {
            var (handlerMock, httpClient) = GetHttpMocks();
            var connection = new FireboltConnection(ConnectionString);
            var client = new FireboltClient2(connection, Guid.NewGuid().ToString(), "password", "http://test.api.firebolt.io", null, "account", httpClient);

            var tokenField = typeof(FireboltClient).GetField("_token", BindingFlags.NonPublic | BindingFlags.Instance);
            tokenField!.SetValue(client, "test_token");

            var queryParamsField = typeof(FireboltClient).GetField("_queryParameters", BindingFlags.NonPublic | BindingFlags.Instance);
            var queryParams = (IDictionary<string, string>)queryParamsField!.GetValue(client)!;
            queryParams["param1"] = "value1";
            queryParams["param2"] = "value2";
            queryParams["param3"] = "value3";
            queryParams["keep_param"] = "keep_value";

            var response = GetResponseMessage("test_result", HttpStatusCode.OK);
            response.Headers.Add("Firebolt-Remove-Parameters", "param1");
            response.Headers.Add("Firebolt-Remove-Parameters", "param2");
            response.Headers.Add("Firebolt-Remove-Parameters", "param3");

            handlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(response);

            await client.ExecuteQueryAsync<string>("endpoint_url", "DBName", "account", "SELECT 1", new HashSet<string>(), CancellationToken.None);
            Assert.Multiple(() =>
            {
                Assert.That(queryParams.ContainsKey("param1"), Is.False, "param1 should have been removed");
                Assert.That(queryParams.ContainsKey("param2"), Is.False, "param2 should have been removed");
                Assert.That(queryParams.ContainsKey("param3"), Is.False, "param3 should have been removed");
                Assert.That(queryParams.ContainsKey("keep_param"), Is.True, "keep_param should remain");
                Assert.That(queryParams["keep_param"], Is.EqualTo("keep_value"));
            });
        }

        [Test]
        public async Task ProcessRemoveParametersHeader_ParametersWithSpaces_RemovesCorrectly()
        {
            var (handlerMock, httpClient) = GetHttpMocks();
            var connection = new FireboltConnection(ConnectionString);
            var client = new FireboltClient2(connection, Guid.NewGuid().ToString(), "password", "http://test.api.firebolt.io", null, "account", httpClient);

            var tokenField = typeof(FireboltClient).GetField("_token", BindingFlags.NonPublic | BindingFlags.Instance);
            tokenField!.SetValue(client, "test_token");

            var queryParamsField = typeof(FireboltClient).GetField("_queryParameters", BindingFlags.NonPublic | BindingFlags.Instance);
            var queryParams = (IDictionary<string, string>)queryParamsField!.GetValue(client)!;
            queryParams["param_with_space"] = "value1";
            queryParams["normal_param"] = "value2";

            var response = GetResponseMessage("test_result", HttpStatusCode.OK);
            response.Headers.Add("Firebolt-Remove-Parameters", " param_with_space ");
            response.Headers.Add("Firebolt-Remove-Parameters", " normal_param ");

            handlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(response);

            await client.ExecuteQueryAsync<string>("endpoint_url", "DBName", "account", "SELECT 1", new HashSet<string>(), CancellationToken.None);
            Assert.Multiple(() =>
            {
                Assert.That(queryParams.ContainsKey("param_with_space"), Is.False, "param_with_space should have been removed");
                Assert.That(queryParams.ContainsKey("normal_param"), Is.False, "normal_param should have been removed");
            });
        }

        [Test]
        public async Task ProcessRemoveParametersHeader_NonExistentParameter_DoesNotThrow()
        {
            var (handlerMock, httpClient) = GetHttpMocks();
            var connection = new FireboltConnection(ConnectionString);
            var client = new FireboltClient2(connection, Guid.NewGuid().ToString(), "password", "http://test.api.firebolt.io", null, "account", httpClient);

            var tokenField = typeof(FireboltClient).GetField("_token", BindingFlags.NonPublic | BindingFlags.Instance);
            tokenField!.SetValue(client, "test_token");

            var queryParamsField = typeof(FireboltClient).GetField("_queryParameters", BindingFlags.NonPublic | BindingFlags.Instance);
            var queryParams = (IDictionary<string, string>)queryParamsField!.GetValue(client)!;
            queryParams["existing_param"] = "value";

            var response = GetResponseMessage("test_result", HttpStatusCode.OK);
            response.Headers.Add("Firebolt-Remove-Parameters", "non_existent_param");

            handlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(response);

            await client.ExecuteQueryAsync<string>("endpoint_url", "DBName", "account", "SELECT 1", new HashSet<string>(), CancellationToken.None);

            Assert.That(queryParams.ContainsKey("existing_param"), Is.True, "existing_param should remain");
        }

        [Test]
        public async Task ProcessRemoveParametersHeader_EmptyParameterInList_IgnoresEmpty()
        {
            var (handlerMock, httpClient) = GetHttpMocks();
            var connection = new FireboltConnection(ConnectionString);
            var client = new FireboltClient2(connection, Guid.NewGuid().ToString(), "password", "http://test.api.firebolt.io", null, "account", httpClient);

            var tokenField = typeof(FireboltClient).GetField("_token", BindingFlags.NonPublic | BindingFlags.Instance);
            tokenField!.SetValue(client, "test_token");

            var queryParamsField = typeof(FireboltClient).GetField("_queryParameters", BindingFlags.NonPublic | BindingFlags.Instance);
            var queryParams = (IDictionary<string, string>)queryParamsField!.GetValue(client)!;
            queryParams["param1"] = "value1";
            queryParams["param2"] = "value2";

            var response = GetResponseMessage("test_result", HttpStatusCode.OK);
            response.Headers.Add("Firebolt-Remove-Parameters", "param1");
            response.Headers.Add("Firebolt-Remove-Parameters", "param2");
            response.Headers.Add("Firebolt-Remove-Parameters", "");

            handlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(response);

            await client.ExecuteQueryAsync<string>("endpoint_url", "DBName", "account", "SELECT 1", new HashSet<string>(), CancellationToken.None);
            Assert.Multiple(() =>
            {
                Assert.That(queryParams.ContainsKey("param1"), Is.False, "param1 should have been removed");
                Assert.That(queryParams.ContainsKey("param2"), Is.False, "param2 should have been removed");
            });
        }

        private static (Mock<HttpMessageHandler> handlerMock, HttpClient httpClient) GetHttpMocks()
        {
            var handlerMock = new Mock<HttpMessageHandler>();
            var httpClient = new HttpClient(handlerMock.Object);
            return (handlerMock, httpClient);
        }

        private static HttpResponseMessage GetResponseMessage(object responseObject, HttpStatusCode httpStatusCode)
        {
            var response = GetResponseMessage(httpStatusCode);
            if (responseObject is string responseAsString)
            {
                response.Content = new StringContent(responseAsString);
            }
            else
            {
                response.Content = new StringContent(SerializeObject(responseObject), Encoding.UTF8, "application/json");
            }
            return response;
        }

        private static HttpResponseMessage GetResponseMessage(HttpStatusCode httpStatusCode)
        {
            return new HttpResponseMessage() { StatusCode = httpStatusCode };
        }
    }
}
