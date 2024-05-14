using System;
using NUnit.Framework;
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Client;
using System.Data;
using System.Data.Common;
using System.Text;
using static System.Environment;

namespace FireboltDotNetSdk.Tests
{
    [SetUpFixture]
    [Category("Integration")]
    internal class IntegrationSetUp //: IntegrationTest
    {
        private string name = "integration_testing__" + DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        private FireboltConnection Connection = null!;

        [OneTimeSetUp]
        public void SetUp()
        {
            if (!bool.Parse(GetEnvironmentVariable("FIREBOLT_RUN_SETUP") ?? "false"))
            {
                return;
            }
            string connectionString = CreateConnectionString(new Tuple<string, string?>[]
            {
                Tuple.Create<string, string?>("Env", GetEnvironmentVariable("FIREBOLT_ENV") ?? "staging"),
                Tuple.Create<string, string?>("Account", GetEnvironmentVariable("FIREBOLT_ACCOUNT") ?? "infra-engines-v2"),
                Tuple.Create<string, string?>("ClientId", GetEnvironmentVariable("FIREBOLT_CLIENT_ID")),
                Tuple.Create<string, string?>("ClientSecret", GetEnvironmentVariable("FIREBOLT_CLIENT_SECRET"))
            });
            Connection = new FireboltConnection(connectionString);
            Connection.Open();
            Perform("FIREBOLT_ENGINE_NAME", "ENGINE", name, "CREATE");
            Perform("FIREBOLT_DATABASE", "DATABASE", name, "CREATE");
        }

        [OneTimeTearDown]
        public void TearDown()
        {
            if (Connection == null)
            {
                return;
            }
            Perform("FIREBOLT_ENGINE_NAME", "ENGINE", name, "STOP", "DROP");
            Perform("FIREBOLT_DATABASE", "DATABASE", name, "DROP");
            Connection.Close();
        }

        private void Perform(string propertyName, string entityType, string entityName, params string[] actions)
        {
            string? propertyValue = GetEnvironmentVariable(propertyName);
            if (propertyValue != null && propertyValue != "")
            {
                return;
            }
            foreach (string action in actions)
            {
                CreateCommand($"{action} {entityType} {entityName}").ExecuteNonQuery();
            }
            SetEnvironmentVariable(propertyName, entityName);
        }

        private DbCommand CreateCommand(string sql)
        {
            DbCommand command = Connection.CreateCommand();
            command.CommandText = sql;
            return command;
        }

        private string CreateConnectionString(params Tuple<string, string?>[] more)
        {
            return string.Join(";", more.Where(p => p.Item2 != null).Select(p => p.Item1 + "=" + p.Item2));
        }
    }
}

