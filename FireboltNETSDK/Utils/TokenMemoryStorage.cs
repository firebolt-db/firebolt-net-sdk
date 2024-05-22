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
            if (lr != null && Convert.ToInt32(lr.Expires_in) < epoch())
            {
                // Token has expired, returning null
                lr = null;
            }
            return Task.FromResult(lr);
        }

        public Task CacheToken(LoginResponse tokenData, string username, string password)
        {
            tokenData.Expires_in = (Convert.ToInt32(tokenData.Expires_in) + epoch()).ToString();
            tokens[CreateKey(username, password)] = tokenData;
            return Task.CompletedTask;
        }

        private string CreateKey(string username, string password)
        {
            return $"{username}:{password}";
        }

        private long epoch()
        {
            return Convert.ToInt32(new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds());
        }
    }
}
