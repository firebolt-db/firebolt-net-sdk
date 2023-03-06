using FireboltDotNetSdk.Exception;
using System.Net;
using System.Text;
using System.Security.Cryptography;

namespace FireboltDotNetSdk.Utils
{
    internal class FernetEncryptor
    {
        private byte[] key;
        public string salt;

        private FernetEncryptor(byte[] key, string salt)
        {
            if (key == null)
            {
                throw new FireboltException($"{nameof(key)} is null.");
            }
            if (key.Length != 32)
            {
                throw new FireboltException($"Length of {nameof(key)} should be 32.");
            }
            if (salt == null)
            {
                throw new FireboltException($"{nameof(salt)} is null.");
            }
            this.key = key;
            this.salt = salt;
        }

        public FernetEncryptor(string password, string salt) : this(generateKey(password, salt), salt)
        { }

        public FernetEncryptor(string password) : this(password, generateSalt())
        { }

        /// <summary>
        /// Generate a random, base64 encoded salt of 32 bytes
        /// </summary>
        /// <returns>Salt</returns>
        private static string generateSalt()
        {
            using (var rng = RandomNumberGenerator.Create())
            {
                var keyBytes = new byte[16];
                rng.GetBytes(keyBytes);
                return Convert.ToBase64String(keyBytes);
            }
        }

        /// <summary>
        /// Generate a key from password and salt using PBKDF2HMAC algorithm
        /// </summary>
        /// <returns>Key</returns>
        private static byte[] generateKey(string password, string salt)
        {
            Rfc2898DeriveBytes pbkdf2 = new Rfc2898DeriveBytes(Encoding.UTF8.GetBytes(password), Convert.FromBase64String(salt), iterations: 39000, System.Security.Cryptography.HashAlgorithmName.SHA256);
            return pbkdf2.GetBytes(32);
        }

        /// <summary>
        /// Encrypts provided data_str with stored key
        /// </summary>
        /// <returns>Encrypted payload in bytes</returns>
        public byte[] Encrypt(string data_str, bool trimEnd = false)
        {
            if (data_str == null)
            {
                throw new FireboltException($"{nameof(data_str)} is null.");
            }

            var data = Encoding.UTF8.GetBytes(data_str);

            var result = new byte[57 + (data.Length + 16) / 16 * 16];
            result[0] = 0x80;

            // Use current timestamp
            var timestamp = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds();
            var timestamp_bytes = BitConverter.GetBytes(IPAddress.NetworkToHostOrder(timestamp));
            Buffer.BlockCopy(timestamp_bytes, 0, result, 1, timestamp_bytes.Length);

            using (var aes = Aes.Create())
            {
                aes.Mode = CipherMode.CBC;

                var encryptionKey = new byte[16];
                Buffer.BlockCopy(this.key, 16, encryptionKey, 0, 16);

                aes.Key = encryptionKey;

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
            Buffer.BlockCopy(this.key, 0, signingKey, 0, 16);

            using (var hmac = new HMACSHA256(signingKey))
            {
                hmac.TransformFinalBlock(result, 0, result.Length - 32);
                var hash = hmac.Hash;
                if (hash == null) throw new FireboltException("Unable to compute hash.");
                Buffer.BlockCopy(hash, 0, result, result.Length - 32, 32);
            }

            return result;
        }

        /// <summary>
        /// Decrypt provided encrypted message with stored key
        /// </summary>
        /// <returns>Decrypted data string</returns>
        public string Decrypt(byte[] encrypted)
        {
            if (encrypted == null)
            {
                throw new FireboltException($"{nameof(encrypted)} is null.");
            }

            if (encrypted.Length < 57)
            {
                throw new FireboltException($"Length of {nameof(key)} should be greater or equal than 57.");
            }

            var version = encrypted[0];

            if (version != 0x80)
            {
                throw new FireboltException("Unsupported encoding version.");
            }

            var signingKey = new byte[16];
            Buffer.BlockCopy(this.key, 0, signingKey, 0, 16);

            using (var hmac = new HMACSHA256(signingKey))
            {
                hmac.TransformFinalBlock(encrypted, 0, encrypted.Length - 32);
                var hash2 = hmac.Hash;
                if (hash2 == null) throw new FireboltException("Wrong HMAC!");

                var hash = encrypted.Skip(encrypted.Length - 32).Take(32);

                if (!hash.SequenceEqual(hash2)) throw new FireboltException("Wrong HMAC!");
            }

            byte[] decrypted;

            using (var aes = Aes.Create())
            {
                aes.Mode = CipherMode.CBC;
                aes.Padding = PaddingMode.PKCS7;

                var encryptionKey = new byte[16];
                Buffer.BlockCopy(this.key, 16, encryptionKey, 0, 16);
                aes.Key = encryptionKey;

                var iv = new byte[16];
                Buffer.BlockCopy(encrypted, 9, iv, 0, 16);
                aes.IV = iv;

                using (var decryptor = aes.CreateDecryptor())
                {
                    const int startCipherText = 25;
                    var cipherTextLength = encrypted.Length - 32 - 25;
                    decrypted = decryptor.TransformFinalBlock(encrypted, startCipherText, cipherTextLength);
                }
            }

            return Encoding.UTF8.GetString(decrypted);
        }
    }
}
