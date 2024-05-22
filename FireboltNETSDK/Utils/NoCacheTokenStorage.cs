using static FireboltDotNetSdk.Client.FireResponse;

namespace FireboltDotNetSdk.Utils
{
    public class NoCacheTokenStorage : TokenStorage
    {
        public Task<LoginResponse?> GetCachedToken(string username, string password)
        {
            return Task.FromResult<LoginResponse?>(null);
        }

        public Task CacheToken(LoginResponse tokenData, string username, string password)
        {
            return Task.CompletedTask;
        }
    }
}
