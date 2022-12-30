using System.Reflection;
using FakeItEasy;
using FireboltDotNetSdk.Client;

namespace FireboltDotNetSdk.Tests;

public class FireboltTokenCachingTest
{
    [Test]
    public void verifyTokenIsFetchedOnlyOnce()
    {
        var connString = $"database=any;username=any;password=any;endpoint=any;account=any";
        var client = A.Fake<IFireboltClient>(x => x.Wrapping(new FireboltClientMock()));
        Type type = typeof(FireboltConnection);
        FieldInfo field = type.GetField("_client");
        var conn = new FireboltConnection(connString);
        field.SetValue(conn, client);
        conn.Open();
        conn.Open();
        A.CallTo(() => client.Login(null,"")).WithAnyArguments().MustHaveHappenedOnceExactly();
    }
    
    public class FireboltClientMock : IFireboltClient
    {
        public Task<FireResponse.LoginResponse> Login(FireRequest.LoginRequest loginRequest, string baseUrl)
        {
            return Task.FromResult(new FireResponse.LoginResponse
            {
                Access_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
            });
        }

        public Task<FireResponse.GetEngineUrlByDatabaseNameResponse> GetEngineUrlByDatabaseName(string? databaseName, string? account, string baseUrl, string? accessToken)
        {
            return Task.FromResult(new FireResponse.GetEngineUrlByDatabaseNameResponse
            {
                Engine_url = "http://hello"
            });
        }

        public Task<FireResponse.GetEngineNameByEngineIdResponse> GetEngineUrlByEngineName(string engine, string? account, string baseUrl, string accessToken)
        {
            throw new NotImplementedException();
        }

        public Task<FireResponse.GetEngineUrlByEngineNameResponse> GetEngineUrlByEngineId(string engineId, string accountId, string baseUrl, string? accessToken)
        {
            throw new NotImplementedException();
        }
    }
}