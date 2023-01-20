using FireboltDotNetSdk.Exception;

namespace FireboltDotNetSdk.Tests
{

    [TestFixture]
    [Category("Integration")]
    [Parallelizable]
    internal class IntegrationTest
    {
        public static string EnvWithDefault(string env_var, string? default_value = null)
        {
            string? env_value = Environment.GetEnvironmentVariable(env_var);
            if (env_value != null)
            {
                return env_value;
            }
            if (default_value == null)
            {
                throw new FireboltException($"Missing {env_var} environment value");
            }
            return default_value;
        }

        protected static string Database = EnvWithDefault("FIREBOLT_DATABASE");
        protected static string Username = EnvWithDefault("FIREBOLT_USERNAME");
        protected static string Password = EnvWithDefault("FIREBOLT_PASSWORD");
        protected static string Endpoint = EnvWithDefault("FIREBOLT_ENDPOINT", "https://api.dev.firebolt.io");
        protected static string Account = EnvWithDefault("FIREBOLT_ACCOUNT", "firebolt");
        protected static string Engine = EnvWithDefault("FIREBOLT_ENGINE_NAME");
        protected static string ClientId = EnvWithDefault("FIREBOLT_CLIENT_ID");
        protected static string ClientSecret = EnvWithDefault("FIREBOLT_CLIENT_SECRET");
    }
}