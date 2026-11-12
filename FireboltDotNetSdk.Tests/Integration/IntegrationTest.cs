using System.Data.Common;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;
using static System.Environment;

namespace FireboltDotNetSdk.Tests
{

    [TestFixture]
    [Category("Integration")]
    [Parallelizable]
    internal class IntegrationTest
    {
        private static string EnvWithDefault(string env_var, string? default_value = null)
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
        protected static string Database = "";
        protected static string? Endpoint;
        protected static string? Env;
        // Endpoint is not specified by CI/CD (YAML) for v2 where account and engine name are mandatory.
        protected static string? Account;
        protected static string? Engine;
        protected static string? ClientId;
        protected static string? ClientSecret;
        protected static string? UserName;
        protected static string? Password;
        protected static string? PreparedStatementParamStyle;

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
            // Let the builder quote values, so that secrets containing characters such as ';' or '"' stay parseable.
            var builder = new DbConnectionStringBuilder();
            foreach (var p in conf.Where(p => p.Value != null))
            {
                builder[p.Key] = p.Value;
            }
            return builder.ConnectionString;
        }

        protected static string ConnectionStringWithout(params string[] names)
        {
            return ConnectionString(Tuples(names));
        }

        protected static Tuple<string, string?>[] Tuples(params string[] names)
        {
            return names.Select(name => new Tuple<string, string?>(name, null)).ToArray();
        }

        protected static IDictionary<string, string?> configuration = new Dictionary<string, string?>();

        [OneTimeSetUp]
        public void SetUp()
        {
            Database = EnvWithDefault("FIREBOLT_DATABASE");
            Endpoint = GetEnvironmentVariable("FIREBOLT_ENDPOINT");
            Env = EnvWithDefault("FIREBOLT_ENV", "staging");
            // Endpoint is not specified by CI/CD (YAML) for v2 where account and engine name are mandatory.
            // Firebolt Core has neither, see OpenConnection.
            var requireAccountAndEngine = Endpoint == null && GetEnvironmentVariable("FIREBOLT_CORE_URL") == null;
            Account = requireAccountAndEngine ? EnvWithDefault("FIREBOLT_ACCOUNT") : GetEnvironmentVariable("FIREBOLT_ACCOUNT");
            Engine = requireAccountAndEngine ? EnvWithDefault("FIREBOLT_ENGINE_NAME") : GetEnvironmentVariable("FIREBOLT_ENGINE_NAME");
            ClientId = GetEnvironmentVariable("FIREBOLT_CLIENT_ID");
            ClientSecret = GetEnvironmentVariable("FIREBOLT_CLIENT_SECRET");
            UserName = GetEnvironmentVariable("FIREBOLT_USERNAME");
            Password = GetEnvironmentVariable("FIREBOLT_PASSWORD");
            PreparedStatementParamStyle = "Native";
            configuration = new Dictionary<string, string?>()
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
                {nameof(PreparedStatementParamStyle).ToLower(), PreparedStatementParamStyle},
            };
        }

        // FIREBOLT_CORE_URL (e.g. http://127.0.0.1:3473) routes the SDK's requests to a local Firebolt Core,
        // which the SDK cannot connect to by itself (no auth, plain http). Use an IPv4 address: the SDK's
        // HTTP client only tries the first address a host name resolves to, and localhost may resolve to ::1.
        protected static FireboltConnection OpenConnection(PreparedStatementParamStyleType style, string? database = null)
        {
            var coreUrl = GetEnvironmentVariable("FIREBOLT_CORE_URL");
            if (coreUrl == null)
            {
                var connection = new FireboltConnection(ConnectionString(
                    new Tuple<string, string?>("preparedStatementParamStyle", style.ToString()),
                    new Tuple<string, string?>(nameof(Database), database ?? Database)));
                connection.Open();
                return connection;
            }
            var coreConnectionString = new DbConnectionStringBuilder
            {
                ["database"] = database ?? Database,
                ["clientid"] = "core",
                ["clientsecret"] = "core",
                ["account"] = "core",
                ["env"] = "core",
                ["preparedStatementParamStyle"] = style.ToString(),
            }.ConnectionString;
            var coreConnection = new FireboltConnection(coreConnectionString)
            {
                EngineUrl = "core",
                InfraVersion = 2,
            };
            coreConnection.Client = new CoreClient(coreConnection, new Uri(coreUrl));
            return coreConnection;
        }

        protected static DbParameter CreateParameter(DbCommand command, string name, object? value)
        {
            DbParameter parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value;
            return parameter;
        }
    }
}
