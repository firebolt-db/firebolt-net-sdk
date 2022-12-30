using FireboltDotNetSdk.Client;

namespace FireboltDotNetSdk;

public interface IFireboltClient
{
    public Task<FireResponse.LoginResponse> Login(FireRequest.LoginRequest loginRequest, string baseUrl);

    public Task<FireResponse.GetEngineUrlByDatabaseNameResponse> GetEngineUrlByDatabaseName(string? databaseName, string? account,
        string baseUrl, string? accessToken);

    public Task<FireResponse.GetEngineNameByEngineIdResponse> GetEngineUrlByEngineName(string engine, string? account,
        string baseUrl, string accessToken);

    public Task<FireResponse.GetEngineUrlByEngineNameResponse> GetEngineUrlByEngineId(string engineId, string accountId,
        string baseUrl, string? accessToken);
}