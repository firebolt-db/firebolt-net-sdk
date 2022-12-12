using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;
using Newtonsoft.Json;
using static FireboltDotNetSdk.Client.FireResponse;

internal static class TokenSecureStorageHelpers
{

    /// <summary>
    /// Encrypt token with user credentials, cache it into file with expiration
    /// </summary>
    public static async Task CacheToken(LoginResponse tokenData, string username, string password)
    {
        try
        {
            var encryptor = new FernetEncryptor(username + password);
            var token = encryptor.Encrypt(tokenData.Access_token);
            CachedJSONData _data = new()
            {
                token = Convert.ToBase64String(token),
                salt = encryptor.salt,
                expiration = Convert.ToInt32(tokenData.Expires_in)
            };

            var cacheDir = TokenSecureStorage.GetCacheDir();
            if (!Directory.Exists(cacheDir))
            {
                Directory.CreateDirectory(cacheDir);
            }
            var cacheFilePath = Path.Join(cacheDir, TokenSecureStorage.GenerateFileName(username, password));

            string json = JsonConvert.SerializeObject(_data, Formatting.Indented);
            await File.WriteAllTextAsync(cacheFilePath, json);
        }
        catch (System.Exception ex)
        {
            throw new FireboltException("Error while caching token: " + ex.Message);
        }
    }
}