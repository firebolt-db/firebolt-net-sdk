using static FireboltDotNetSdk.Client.FireResponse;
using System.Collections.Concurrent;

namespace FireboltDotNetSdk.Utils
{
    public class TokenMemoryStorage : TokenStorage
    {
        internal static IDictionary<string, LoginResponse> tokens = new ConcurrentDictionary<string, LoginResponse>();

        public Task<LoginResponse?> GetCachedToken(string username, string password)
        {
            LoginResponse? lr;
            tokens.TryGetValue(CreateKey(username, password), out lr);
            if (lr != null && Convert.ToInt64(lr.Expires_in) < Constant.GetCurrentEpoch())
            {
                // Token has expired, returning null
                lr = null;
            }
            return Task.FromResult(lr);
        }

        public Task CacheToken(LoginResponse tokenData, string username, string password)
        {
            tokens[CreateKey(username, password)] = tokenData;
            return Task.CompletedTask;
        }

        private string CreateKey(string username, string password)
        {
            return $"{username}:{password}";
        }
    }
}
