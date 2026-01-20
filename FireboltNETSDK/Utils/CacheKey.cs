namespace FireboltDotNetSdk.Utils
{
    /// <summary>
    /// Represents a cache key used to identify unique connection cache entries based on client credentials and account.
    /// Matches JDBC ClientSecretCacheKey structure for easier disk cache implementation.
    /// </summary>
    public class CacheKey
    {
        private readonly string _value;

        public CacheKey(string clientId, string clientSecret, string account)
        {
            if (clientId == null) throw new ArgumentNullException(nameof(clientId));
            if (clientSecret == null) throw new ArgumentNullException(nameof(clientSecret));
            if (account == null) throw new ArgumentNullException(nameof(account));

            _value = HashValues(clientId, clientSecret, account);
        }

        /// <summary>
        /// Gets the cache key value (hash of clientId, clientSecret, and account).
        /// </summary>
        public string GetValue()
        {
            return _value;
        }

        private static string HashValues(string clientId, string clientSecret, string account)
        {
            return string.Join("#", clientId, clientSecret, account);
        }
    }
}
