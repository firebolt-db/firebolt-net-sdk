using FireboltDotNetSdk.Exception;
using static System.Environment;

namespace FireboltDotNetSdk.Tests
{

    [TestFixture]
    [Category("Integration")]
    [Parallelizable]
    internal class IntegrationTest
    {
        public static string EnvWithDefault(string env_var, string? default_value = null)
        {
            string? env_value = GetEnvironmentVariable(env_var);
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
        protected static string? Endpoint = GetEnvironmentVariable("FIREBOLT_ENDPOINT");
        protected static string Env = EnvWithDefault("FIREBOLT_ENV", "dev");
        protected static string? Account = Endpoint == null ? EnvWithDefault("FIREBOLT_ACCOUNT") : GetEnvironmentVariable("FIREBOLT_ACCOUNT");
        protected static string? Engine = Endpoint == null ? EnvWithDefault("FIREBOLT_ENGINE_NAME") : GetEnvironmentVariable("FIREBOLT_ENGINE_NAME");
        protected static string? ClientId = GetEnvironmentVariable("FIREBOLT_CLIENT_ID");
        protected static string? ClientSecret = GetEnvironmentVariable("FIREBOLT_CLIENT_SECRET");
        protected static string? UserName = GetEnvironmentVariable("FIREBOLT_USERNAME");
        protected static string? Password = GetEnvironmentVariable("FIREBOLT_PASSWORD");

        // protected static string ConnectionString()
        // {
        //     return ConnectionStringWithout();
        // }

        protected static string ConnectionString(params Tuple<string, string?>[] more)
        {
            IDictionary<string, string?> conf = configuration;
            if (more.Length > 0)
            {
                IDictionary<string, string?> fixes = more.ToDictionary(p => p.Item1.ToLower(), p => p.Item2);
                conf = configuration
                .Select(p => new Tuple<string, string?>(p.Key, fixes.ContainsKey(p.Key) ? fixes[p.Key] : p.Value))
                .Where(p => p.Item2 != null)
                .ToDictionary(p => p.Item1, p => p.Item2);
            }
            return string.Join(";", conf.Where(p => p.Value != null).Select(p => p.Key + "=" + p.Value));
        }
        protected static string ConnectionStringWithout(params string[] names)
        {
            return ConnectionString(Tuples(names));
        }

        protected static Tuple<string, string?>[] Tuples(params string[] names)
        {
            return names.Select(name => new Tuple<string, string?>(name, null)).ToArray();
        }

        protected static IDictionary<string, string?> configuration = new Dictionary<string, string?>()
        {
            {nameof(Database).ToLower(), Database},
            {nameof(Endpoint).ToLower(), Endpoint},
            {nameof(Env).ToLower(), Env},
            {nameof(Account).ToLower(), Account},
            {nameof(Engine).ToLower(), Engine},
            {nameof(ClientId).ToLower(), ClientId},
            {nameof(ClientSecret).ToLower(), ClientSecret},
            {nameof(UserName).ToLower(), UserName},
            {nameof(Password).ToLower(), Password},
        };
    }
}
