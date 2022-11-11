using FireboltDotNetSdk.Exception;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using static FireboltDotNetSdk.Client.FireResponse;

namespace FireboltDotNetSdk.Utils
{
    public static class TokenSecureStorage
    {
        private static readonly string APPNAME = "firebolt";
        private static string FileName;

        /// <summary>
        /// Identify filesystem path where we srote JSON file
        /// </summary>
        /// <returns>filesystem path</returns>

        public static string GetCacheDir()
        {
            var userLocalDir = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
            return Path.Join(userLocalDir, APPNAME);
        }

        public static string GenerateFileName(string username, string password)
        {
            var usernameBytes = SHA256HexHashString(username);
            var passwordBytes = SHA256HexHashString(password);
            return usernameBytes + passwordBytes + ".json";
        }

        private static string ToHex(byte[] bytes, bool upperCase)
        {
            int bytesLenght = 16;
            StringBuilder result = new StringBuilder(bytesLenght * 2);
            for (int i = 0; i < bytesLenght; i++)
                result.Append(bytes[i].ToString(upperCase ? "X2" : "x2"));
            return result.ToString();
        }

        public static string SHA256HexHashString(string StringIn)
        {
            string hashString;
            using (var sha256 = SHA256.Create())
            {
                var hash = sha256.ComputeHash(Encoding.Default.GetBytes(StringIn));
                hashString = ToHex(hash, false);
            }

            return hashString;
        }

        /// <summary>
        /// Get cached token from file
        /// </summary>
        /// <returns></returns>
        public static CachedJSONData? GetCachedToken(string username, string password)
        {
            var cacheFilePath = Path.Join(GetCacheDir(), GenerateFileName(username, password));
            try
            {
                var data = ReadDataJSON(cacheFilePath);
                if (data == null) return null;

                var key64 = data.salt.UrlSafe64Decode();
                var decoded64 = Decrypt(key64, data.token, out var timestamp);
                var decoded = decoded64.UrlSafe64Encode().FromBase64String();

                CachedJSONData _data = new()
                {
                    token = decoded,
                    salt = data.salt,
                    expiration = Convert.ToInt32(data.expiration)
                };
                return _data;
            }
            catch (System.Exception ex)
            {
                throw new FireboltException("Error while GetCachedToken " + ex.Message);
            }
        }

        /// <summary>
        /// Cached token and set to file
        /// </summary>
        /// <returns></returns>
        public static bool CachedTokenAsync(LoginResponse tokenData, string username, string password)
        {

            var salt = GenerateSalt();
            var key = GenerateKey(salt, username + password);
            var tokenPayload = tokenData.Access_token;

            try
            {
                var token = Encrypt(key.UrlSafe64Decode(), Encoding.Unicode.GetBytes(tokenPayload));
                CachedJSONData _data = new()
                {
                    token = token,
                    salt = key,
                    expiration = Convert.ToInt32(tokenData.Expires_in)
                };

                var cacheDir = GetCacheDir();
                if (!Directory.Exists(cacheDir))
                {
                    Directory.CreateDirectory(cacheDir);
                }
                var cacheFilePath = Path.Join(cacheDir, GenerateFileName(username, password));

                string json = JsonConvert.SerializeObject(_data, Formatting.Indented);
                File.WriteAllText(cacheFilePath, json);
                return true;
            }
            catch (System.Exception ex)
            {
                throw new FireboltException("Error while CachedTokenAsync " + ex.Message);
            }
        }

        /// <summary>
        /// Read JSON file
        /// </summary>
        /// <returns>Deserialize JSON</returns>
        public static CachedJSONData ReadDataJSON(string path)
        {
            if (!Directory.Exists(path))
            {
                return null;
            }

            try
            {
                StreamReader r = new StreamReader(path);
                string jsonString = r.ReadToEnd();
                var prettyJson = JToken.Parse(jsonString).ToString(Formatting.Indented);
                var data = JsonConvert.DeserializeObject<CachedJSONData>(prettyJson);
                return data;
            }
            catch (System.Exception ex)
            {
                throw new FireboltException("Error while ReadDataJSON " + ex.Message);
            }
        }

        /// <summary>
        /// Encrypt method 
        /// </summary>
        /// <returns>Encryption token</returns>
        public static string Encrypt(byte[] key, byte[] data, DateTime? timestamp = null, byte[] iv = null,
            bool trimEnd = false)
        {
            if (key == null)
            {
                throw new FireboltException($"{nameof(key)} is null.");
            }

            if (key.Length != 32)
            {
                throw new FireboltException($"Length of {nameof(key)} should be 32.");
            }

            if (data == null)
            {
                throw new FireboltException($"{nameof(data)} is null.");
            }

            if (iv != null && iv.Length != 16)
            {
                throw new FireboltException($"Length of {nameof(iv)} should be 16.");
            }

            if (timestamp == null) timestamp = DateTime.UtcNow;

            var result = new byte[57 + (data.Length + 16) / 16 * 16];

            result[0] = 0x80;

            var timestamp2 = new DateTimeOffset(timestamp.Value).ToUnixTimeSeconds();
            timestamp2 = IPAddress.NetworkToHostOrder(timestamp2);
            var timestamp3 = BitConverter.GetBytes(timestamp2);
            Buffer.BlockCopy(timestamp3, 0, result, 1, timestamp3.Length);

            using (var aes = new AesManaged())
            {
                aes.Mode = CipherMode.CBC;

                var encryptionKey = new byte[16];
                Buffer.BlockCopy(key, 16, encryptionKey, 0, 16);

                aes.Key = encryptionKey;

                if (iv != null)
                    aes.IV = iv;
                else
                    aes.GenerateIV();

                Buffer.BlockCopy(aes.IV, 0, result, 9, 16);

                aes.Padding = PaddingMode.PKCS7;

                using (var encryptor = aes.CreateEncryptor())
                {
                    var encrypted = encryptor.TransformFinalBlock(data, 0, data.Length);
                    Buffer.BlockCopy(encrypted, 0, result, 25, encrypted.Length);
                }
            }

            var signingKey = new byte[16];
            Buffer.BlockCopy(key, 0, signingKey, 0, 16);

            using (var hmac = new HMACSHA256(signingKey))
            {
                hmac.TransformFinalBlock(result, 0, result.Length - 32);
                Buffer.BlockCopy(hmac.Hash, 0, result, result.Length - 32, 32);
            }

            return result.UrlSafe64Encode(trimEnd);
        }

        /// <summary>
        /// Decrypt method 
        /// </summary>
        /// <returns>Decryption token</returns>
        public static byte[] Decrypt(byte[] key, string token, out DateTime timestamp, int? ttl = null)
        {
            if (key == null)
            {
                throw new FireboltException($"{nameof(key)} is null.");
            }

            if (key.Length != 32)
            {
                throw new FireboltException($"Length of {nameof(key)} should be 32.");
            }

            if (token == null)
            {
                throw new FireboltException($"{nameof(key)} is null.");
            }

            var token2 = token.UrlSafe64Decode();

            if (token2.Length < 57) throw new FireboltException($"Length of {nameof(key)} should be greater or equal 57.");

            var version = token2[0];

            if (version != 0x80) throw new FireboltException("Invalid version.");

            var signingKey = new byte[16];
            Buffer.BlockCopy(key, 0, signingKey, 0, 16);

            using (var hmac = new HMACSHA256(signingKey))
            {
                hmac.TransformFinalBlock(token2, 0, token2.Length - 32);
                var hash2 = hmac.Hash;

                var hash = token2.Skip(token2.Length - 32).Take(32);

                if (!hash.SequenceEqual(hash2)) throw new FireboltException("Wrong HMAC!");
            }

            var timestamp2 = BitConverter.ToInt64(token2, 1);
            timestamp2 = IPAddress.NetworkToHostOrder(timestamp2);
            var datetimeOffset = DateTimeOffset.FromUnixTimeSeconds(timestamp2);
            timestamp = datetimeOffset.UtcDateTime;

            if (ttl.HasValue)
            {
                var calculatedTimeSeconds = datetimeOffset.ToUnixTimeSeconds() + ttl.Value;
                var currentTimeSeconds = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds();
                if (calculatedTimeSeconds < currentTimeSeconds)
                {
                    throw new FireboltException("Token is expired.");
                }
            }

            byte[] decrypted;

            using (var aes = new AesManaged())
            {
                aes.Mode = CipherMode.CBC;
                aes.Padding = PaddingMode.PKCS7;

                var encryptionKey = new byte[16];
                Buffer.BlockCopy(key, 16, encryptionKey, 0, 16);
                aes.Key = encryptionKey;

                var iv = new byte[16];
                Buffer.BlockCopy(token2, 9, iv, 0, 16);
                aes.IV = iv;

                using (var decryptor = aes.CreateDecryptor())
                {
                    const int startCipherText = 25;
                    var cipherTextLength = token2.Length - 32 - 25;
                    decrypted = decryptor.TransformFinalBlock(token2, startCipherText, cipherTextLength);
                }
            }

            return decrypted;
        }

        /// <summary>
        /// Random Salt Creation
        /// </summary>
        /// <returns>A random salt of the required size.</returns>
        public static string GenerateSalt()
        {
            var keyBytes = new byte[16];
            var rng = new RNGCryptoServiceProvider();
            rng.GetBytes(keyBytes);
            return keyBytes.UrlSafe64Encode();
        }

        public static string GenerateKey(string salt, string password)
        {
            using (var hmac = new HMACSHA256())
            {
                var df = new Pbkdf2(hmac, password, salt.UrlSafe64Decode(), 49000);
                return Convert.ToBase64String(df.GetBytes(16));
            }
        }

        public static string UrlSafe64Encode(this byte[] bytes, bool trimEnd = false)
        {
            try
            {
                var length = (bytes.Length + 2) / 3 * 4;
                var chars = new char[length];
                Convert.ToBase64CharArray(bytes, 0, bytes.Length, chars, 0);

                var trimmedLength = length;

                if (trimEnd)
                    switch (bytes.Length % 3)
                    {
                        case 1:
                            trimmedLength -= 2;
                            break;
                        case 2:
                            trimmedLength -= 1;
                            break;
                    }

                for (var i = 0; i < trimmedLength; i++)
                    switch (chars[i])
                    {
                        case '/':
                            chars[i] = '_';
                            break;
                        case '+':
                            chars[i] = '-';
                            break;
                    }

                var result = new string(chars, 0, trimmedLength);
                return result;
            }
            catch (IOException e)
            {
                throw new FireboltException(e.Message);
            }
        }

        public static byte[] UrlSafe64Decode(this string s)
        {
            try
            {
                char[] chars;

                switch (s.Length % 4)
                {
                    case 2:
                        chars = new char[s.Length + 2];
                        chars[chars.Length - 2] = '=';
                        chars[chars.Length - 1] = '=';
                        break;
                    case 3:
                        chars = new char[s.Length + 1];
                        chars[chars.Length - 1] = '=';
                        break;
                    default:
                        chars = new char[s.Length];
                        break;
                }

                for (var i = 0; i < s.Length; i++)
                    switch (s[i])
                    {
                        case '_':
                            chars[i] = '/';
                            break;
                        case '-':
                            chars[i] = '+';
                            break;
                        default:
                            chars[i] = s[i];
                            break;
                    }

                var result = Convert.FromBase64CharArray(chars, 0, chars.Length);
                return result;
            }
            catch (IOException e)
            {
                throw new FireboltException(e.Message);
            }
        }

        public static string ToBase64String(this string plainText)
        {
            var plainTextBytes = System.Text.Encoding.UTF8.GetBytes(plainText);
            return System.Convert.ToBase64String(plainTextBytes);
        }

        public static string FromBase64String(this string base64EncodedData)
        {
            var base64EncodedBytes = Convert.FromBase64String(base64EncodedData);
            return System.Text.Encoding.UTF8.GetString(base64EncodedBytes);
        }
    }

    public class CachedJSONData
    {
        public string token { get; set; }
        public string salt { get; set; }
        public int expiration { get; set; }
    }
}
