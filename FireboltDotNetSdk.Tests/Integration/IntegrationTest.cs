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

        protected static string Database = EnvWithDefault("FIREBOLT_DATABASE", "alex_test2_2");
        protected static string Endpoint = EnvWithDefault("FIREBOLT_ENDPOINT", "https://api.dev.firebolt.io");
        protected static string Env = EnvWithDefault("FIREBOLT_ENV", "dev");
        protected static string Account = EnvWithDefault("FIREBOLT_ACCOUNT", "developer");
        protected static string EngineName = EnvWithDefault("FIREBOLT_ENGINE_NAME", "alex_test2_2_Ingest");
        protected static string ClientId = EnvWithDefault("FIREBOLT_CLIENT_ID", "fcJFhHEFTcTKlenUd2zNXFoVoR501BtF");
        protected static string ClientSecret = EnvWithDefault("FIREBOLT_CLIENT_SECRET", "FigJDVxl1ZceNK9pJ7b-Y3Q9_kV3T8pOv7stSNuh-d0ujIdH87RAY65xFe8sNZBv");
    }
}
