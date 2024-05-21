using System.Security.Cryptography;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Microsoft.AspNetCore.WebUtilities;
using FireboltDotNetSdk.Exception;
using static FireboltDotNetSdk.Client.FireResponse;

namespace FireboltDotNetSdk.Utils
{
    public class TokenSecureStorage : TokenStorage
    {
        private static readonly string APPNAME = "firebolt";

        private string SHA256HexHashString(string StringIn)
        {
            using (var sha256 = SHA256.Create())
            {
                return Convert.ToHexString(sha256.ComputeHash(Encoding.UTF8.GetBytes(StringIn))).Substring(0, 32).ToLower();
            }
        }

        /// <summary>
        /// Get OS-specific user cache directory path for Firebolt
        /// </summary>
        /// <returns>User cache directory path</returns>
        private string GetCacheDir()
        {
            var userLocalDir = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
            return Path.Join(userLocalDir, APPNAME);
        }

        /// <summary>
        /// Generate cache file name based on user credentials
        /// </summary>
        /// <returns>File name</returns>
        private string GenerateFileName(string username, string password)
        {
            var usernameBytes = SHA256HexHashString(username);
            var passwordBytes = SHA256HexHashString(password);
            return usernameBytes + passwordBytes + ".json";
        }

        /// <summary>
        /// Get token data from cache if any
        /// </summary>
        /// <returns>Token data</returns>
        public async Task<LoginResponse?> GetCachedToken(string username, string password)
        {
            var cacheFilePath = Path.Join(GetCacheDir(), GenerateFileName(username, password));
            try
            {
                var raw_data = await readDataJSONAsync(cacheFilePath);
                if (raw_data == null) return null;

                var b64decoded = WebEncoders.Base64UrlDecode(raw_data.token);
                var token = (new FernetEncryptor(username + password, raw_data.salt)).Decrypt(b64decoded);

                CachedJSONData data = new(
                    paramToken: token,
                    paramSalt: raw_data.salt,
                    paramExpiration: Convert.ToInt32(raw_data.expiration)
                );
                if (data.expiration < Convert.ToInt32(new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds()))
                {
                    // Token has expired, returning null
                    return null;
                }

                return new LoginResponse(access_token: data.token, expires_in: data.expiration.ToString(), token_type: "");
            }
            catch (System.Exception ex)
            {
                throw new FireboltException("Error while reading cached token", ex);
            }
        }

        /// <summary>
        /// Encrypt token with user credentials, cache it into file with expiration
        /// </summary>
        public async Task CacheToken(LoginResponse tokenData, string username, string password)
        {
            try
            {
                var encryptor = new FernetEncryptor(username + password);
                var token = encryptor.Encrypt(tokenData.Access_token);
                CachedJSONData data = new(
                    paramToken: WebEncoders.Base64UrlEncode(token),
                    paramSalt: encryptor.salt,
                    paramExpiration: Convert.ToInt32(tokenData.Expires_in) + Convert.ToInt32(new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds())
                );

                var cacheDir = GetCacheDir();
                if (!Directory.Exists(cacheDir))
                {
                    Directory.CreateDirectory(cacheDir);
                }
                var cacheFilePath = Path.Join(cacheDir, GenerateFileName(username, password));

                string json = JsonConvert.SerializeObject(data, Formatting.Indented);
                await File.WriteAllTextAsync(cacheFilePath, json);
            }
            catch (System.Exception ex)
            {
                throw new FireboltException("Error while caching token", ex);
            }
        }

        /// <summary>
        /// Read JSON data from provided file
        /// </summary>
        /// <returns>Deserialized JSON</returns>
        private async Task<CachedJSONData?> readDataJSONAsync(string path)
        {
            if (!File.Exists(path)) return null;

            try
            {
                StreamReader r = new StreamReader(path);
                string jsonString = await r.ReadToEndAsync();
                var prettyJson = JToken.Parse(jsonString).ToString(Formatting.Indented);
                var data = JsonConvert.DeserializeObject<CachedJSONData>(prettyJson);
                return data;
            }
            catch (System.Exception ex)
            {
                throw new FireboltException("Error while reading/deserializing JSON data ", ex);
            }
        }
    }

    public class CachedJSONData
    {
        public CachedJSONData(string paramToken, string paramSalt, int paramExpiration)
        {
            token = paramToken;
            salt = paramSalt;
            expiration = paramExpiration;
        }
        public string token { get; set; }
        public string salt { get; set; }
        public int expiration { get; set; }
    }
}
