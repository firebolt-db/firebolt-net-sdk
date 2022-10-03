using FireboltDotNetSdk.Exception;
using Newtonsoft.Json;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Generators;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Security;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using static FireboltDotNetSdk.Client.FireResponse;

namespace FireboltDotNetSdk.Utils
{
    public static class TokenSecureStorage
    {
        private static string APPNAME = "\\firebolt";
        private static Random random = new Random();
        private static string FileName;

        public static string GetOperatingSystem()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                return Environment.GetEnvironmentVariable("Home") + APPNAME;
            }

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                return Environment.GetEnvironmentVariable("Home") + APPNAME;
            }

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                return Environment.GetEnvironmentVariable("LocalAppData") + APPNAME;
            }

            throw new FireboltException("Cannot determine operating system!");
        }

        public static string GenerateSalt() {
            ASCIIEncoding ascii = new ASCIIEncoding();

            return Base64Encode(RandomString(16)); // decode(ASCII)  why????
        }

        public static string GenerateFileName(string username, string password) {
            var usernameBytes = Convert.ToBase64String(SHA256.Create().ComputeHash(Encoding.UTF8.GetBytes(username)));
            var passwordBytes = Convert.ToBase64String(SHA256.Create().ComputeHash(Encoding.UTF8.GetBytes(password)));
            FileName = usernameBytes + passwordBytes + ".json";
            return FileName;
        }

        public static bool CreatePasswordHash_Single()
        {
            int iterations = 39000; 
            int saltByteSize = 32; 
            int hashByteSize = 64; 

            BouncyCastleHashing mainHashingLib = new BouncyCastleHashing();

            var password = "password";
            var username = "username";
            var creds = username + password;

            byte[] saltBytes = mainHashingLib.CreateSalt(saltByteSize);
            string saltString = Convert.ToBase64String(saltBytes);

            string pwdHash = mainHashingLib.PBKDF2_SHA256_GetHash(creds, saltString, iterations, hashByteSize);

            var isValid = mainHashingLib.ValidatePassword(creds, saltBytes, iterations, hashByteSize, Convert.FromBase64String(pwdHash));

            return isValid;
        }

        public static string GetSalt() {
            return null;
        }

        public static CachedJSONData GetCachedToken(string path) {
            var data = ReadDataJSON(path);

            if (data == null)
            {
                return null;
            }

            return data;
        }

        public static bool CachedTokenAsync(string dir , LoginResponse tokenData, string username, string password) {

            List<CachedJSONData> _data = new();
            _data.Add(new CachedJSONData()
            {
                token = tokenData.Access_token,
                salt = GenerateSalt(),
                expiration = Convert.ToInt32(tokenData.Expires_in)
            });
           
            var getFileName = dir + "\\" + GenerateFileName(username, password).Replace(@"/", string.Empty);
            string json = JsonConvert.SerializeObject(_data.ToArray());

            //write string to file
            System.IO.File.WriteAllText(getFileName, json);

            return true;
        }

        public static CachedJSONData ReadDataJSON(string path) {
            StreamReader r = new StreamReader(path);
            string jsonString = r.ReadToEnd();
            var data = JsonConvert.DeserializeObject<CachedJSONData>(jsonString);
            return data;
        }

        public static string RandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789=";
            return new string(Enumerable.Repeat(chars, length)
                .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        public static string Base64Encode(string plainText)
        {
            var plainTextBytes = Encoding.UTF8.GetBytes(plainText);
            return Convert.ToBase64String(plainTextBytes);
        }

        public static string Base64Decode(string base64EncodedData)
        {
            var base64EncodedBytes = Convert.FromBase64String(base64EncodedData);
            return Encoding.UTF8.GetString(base64EncodedBytes);
        }
    }
    public class CachedJSONData
    {
        public string token { get; set; }
        public string salt { get; set; }
        public int expiration { get; set; }
    }

    /// <summary>
    /// Contains the relevant Bouncy Castle Methods required to encrypt a password.
    /// References NuGet Package BouncyCastle.Crypto.dll
    /// </summary>
    public class BouncyCastleHashing
    {
        private SecureRandom _cryptoRandom;

        public BouncyCastleHashing()
        {
            _cryptoRandom = new SecureRandom();
        }

        /// <summary>
        /// Random Salt Creation
        /// </summary>
        /// <param name="size">The size of the salt in bytes</param>
        /// <returns>A random salt of the required size.</returns>
        public byte[] CreateSalt(int size)
        {
            byte[] salt = new byte[size];
            _cryptoRandom.NextBytes(salt);
            return salt;
        }

        /// <summary>
        /// Gets a PBKDF2_SHA256 Hash  (Overload)
        /// </summary>
        /// <param name="password">The password as a plain text string</param>
        /// <param name="saltAsBase64String">The salt for the password</param>
        /// <param name="iterations">The number of times to encrypt the password</param>
        /// <param name="hashByteSize">The byte size of the final hash</param>
        /// <returns>A base64 string of the hash.</returns>
        public string PBKDF2_SHA256_GetHash(string password, string saltAsBase64String, int iterations, int hashByteSize)
        {
            var saltBytes = Convert.FromBase64String(saltAsBase64String);

            var hash = PBKDF2_SHA256_GetHash(password, saltBytes, iterations, hashByteSize);

            return Convert.ToBase64String(hash);
        }

        /// <summary>
        /// Gets a PBKDF2_SHA256 Hash (CORE METHOD)
        /// </summary>
        /// <param name="password">The password as a plain text string</param>
        /// <param name="salt">The salt as a byte array</param>
        /// <param name="iterations">The number of times to encrypt the password</param>
        /// <param name="hashByteSize">The byte size of the final hash</param>
        /// <returns>A the hash as a byte array.</returns>
        public byte[] PBKDF2_SHA256_GetHash(string password, byte[] salt, int iterations, int hashByteSize)
        {
            var pdb = new Pkcs5S2ParametersGenerator(new Org.BouncyCastle.Crypto.Digests.Sha256Digest());
            pdb.Init(PbeParametersGenerator.Pkcs5PasswordToBytes(password.ToCharArray()), salt,
                         iterations);
            var key = (KeyParameter)pdb.GenerateDerivedMacParameters(hashByteSize * 8);
            return key.GetKey();
        }

        /// <summary>
        /// Validates a password given a hash of the correct one. (OVERLOAD)
        /// </summary>
        /// <param name="password">The original password to hash</param>
        /// <param name="salt">The salt that was used when hashing the password</param>
        /// <param name="iterations">The number of times it was encrypted</param>
        /// <param name="hashByteSize">The byte size of the final hash</param>
        /// <param name="hashAsBase64String">The hash the password previously provided as a base64 string</param>
        /// <returns>True if the hashes match</returns>
        public bool ValidatePassword(string password, string salt, int iterations, int hashByteSize, string hashAsBase64String)
        {
            byte[] saltBytes = Convert.FromBase64String(salt);
            byte[] actualHashBytes = Convert.FromBase64String(hashAsBase64String);
            return ValidatePassword(password, saltBytes, iterations, hashByteSize, actualHashBytes);
        }

        /// <summary>
        /// Validates a password given a hash of the correct one (MAIN METHOD).
        /// </summary>
        /// <param name="password">The password to check.</param>
        /// <param name="correctHash">A hash of the correct password.</param>
        /// <returns>True if the password is correct. False otherwise.</returns>
        public bool ValidatePassword(string password, byte[] saltBytes, int iterations, int hashByteSize, byte[] actualGainedHasAsByteArray)
        {
            byte[] testHash = PBKDF2_SHA256_GetHash(password, saltBytes, iterations, hashByteSize);
            return SlowEquals(actualGainedHasAsByteArray, testHash);
        }

        /// <summary>
        /// Compares two byte arrays in length-constant time. This comparison
        /// method is used so that password hashes cannot be extracted from
        /// on-line systems using a timing attack and then attacked off-line.
        /// </summary>
        /// <param name="a">The first byte array.</param>
        /// <param name="b">The second byte array.</param>
        /// <returns>True if both byte arrays are equal. False otherwise.</returns>
        private bool SlowEquals(byte[] a, byte[] b)
        {
            uint diff = (uint)a.Length ^ (uint)b.Length;
            for (int i = 0; i < a.Length && i < b.Length; i++)
                diff |= (uint)(a[i] ^ b[i]);
            return diff == 0;
        }

    }
}
