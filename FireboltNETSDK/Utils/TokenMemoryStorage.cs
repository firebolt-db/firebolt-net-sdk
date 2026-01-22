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
            // Convert relative expiry to absolute expiry before storing
            var absoluteExpiry = (Convert.ToInt32(tokenData.Expires_in) + Constant.GetCurrentEpoch()).ToString();
            tokens[CreateKey(username, password)] = new LoginResponse(tokenData.Access_token, absoluteExpiry, tokenData.Token_type);
            return Task.CompletedTask;
        }

        private string CreateKey(string username, string password)
        {
            return $"{username}:{password}";
        }
    }
}
