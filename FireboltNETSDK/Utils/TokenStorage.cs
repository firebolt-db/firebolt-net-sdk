using static FireboltDotNetSdk.Client.FireResponse;

namespace FireboltDotNetSdk.Utils
{
    public interface TokenStorage
    {
        public Task<LoginResponse?> GetCachedToken(string username, string password);
        public Task CacheToken(LoginResponse tokenData, string username, string password);

        private static IDictionary<TokenStorageType, TokenStorage> tokenStorage = new Dictionary<TokenStorageType, TokenStorage>
        {
            { TokenStorageType.None, new NoCacheTokenStorage() },
            { TokenStorageType.Memory, new TokenMemoryStorage() },
            { TokenStorageType.File, new TokenSecureStorage() },
        };


        public static TokenStorage create(TokenStorageType type)
        {
            return tokenStorage[type];
        }
    }

    public enum TokenStorageType
    {
        None,
        Memory,
        File,
    }
}
