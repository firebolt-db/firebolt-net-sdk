using System.Security.Cryptography;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using FireboltDotNetSdk.Exception;
using static FireboltDotNetSdk.Client.FireResponse;

namespace FireboltDotNetSdk.Utils
{
    public static class TokenSecureStorage
    {
        private static readonly string APPNAME = "firebolt";

        private static string SHA256HexHashString(string StringIn)
        {
            using (var sha256 = SHA256.Create())
            {
                return Convert.ToHexString(
            sha256.ComputeHash(Encoding.UTF8.GetBytes(StringIn))).Substring(0, 32).ToLower();
            }
        }

        /// <summary>
        /// Get OS-specific user cache directory path for Firebolt
        /// </summary>
        /// <returns>User cache directory path</returns>
        public static string GetCacheDir()
        {
            var userLocalDir = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
            return Path.Join(userLocalDir, APPNAME);
        }

        /// <summary>
        /// Generate cache file name based on user credentials
        /// </summary>
        /// <returns>File name</returns>
        public static string GenerateFileName(string username, string password)
        {
            var usernameBytes = SHA256HexHashString(username);
            var passwordBytes = SHA256HexHashString(password);
            return usernameBytes + passwordBytes + ".json";
        }

        /// <summary>
        /// Get token data from cache if any
        /// </summary>
        /// <returns>Token data</returns>
        public static async Task<CachedJSONData?> GetCachedToken(string username, string password)
        {
            var cacheFilePath = Path.Join(GetCacheDir(), GenerateFileName(username, password));
            try
            {
                var data = await readDataJSONAsync(cacheFilePath);
                if (data == null) return null;

                var token = (new FernetEncryptor(username + password, data.salt)).Decrypt(Convert.FromBase64String(data.token));

                CachedJSONData _data = new()
                {
                    token = token,
                    salt = data.salt,
                    expiration = Convert.ToInt32(data.expiration)
                };
                return _data;
            }
            catch (System.Exception ex)
            {
                throw new FireboltException("Error while reading token cache: " + ex.Message);
            }
        }

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

                var cacheDir = GetCacheDir();
                if (!Directory.Exists(cacheDir))
                {
                    Directory.CreateDirectory(cacheDir);
                }
                var cacheFilePath = Path.Join(cacheDir, GenerateFileName(username, password));

                string json = JsonConvert.SerializeObject(_data, Formatting.Indented);
                await File.WriteAllTextAsync(cacheFilePath, json);
            }
            catch (System.Exception ex)
            {
                throw new FireboltException("Error while caching token: " + ex.Message);
            }
        }

        /// <summary>
        /// Read JSON data from provided file
        /// </summary>
        /// <returns>Deserialized JSON</returns>
        private static async Task<CachedJSONData?> readDataJSONAsync(string path)
        {
            if (!Directory.Exists(path))
            {
                return null;
            }

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
                throw new FireboltException("Error while ReadDataJSON " + ex.Message);
            }
        }
    }

    public class CachedJSONData
    {
        public string token { get; set; }
        public string salt { get; set; }
        public int expiration { get; set; }
    }
}
