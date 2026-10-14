using System.Reflection;

namespace FireboltDotNetSdk.Tests.Helpers
{
    internal static class FireboltClientExtensions
    {
        /// <summary>
        /// Gives the client an access token, so tests can send queries without mocking the authentication request.
        /// </summary>
        public static void SetToken(this FireboltClient client, string token)
        {
            typeof(FireboltClient).GetField("_token", BindingFlags.NonPublic | BindingFlags.Instance)!.SetValue(client, token);
        }
    }
}
