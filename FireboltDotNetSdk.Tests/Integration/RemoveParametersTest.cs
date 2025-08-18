using System.Reflection;
using FireboltDotNetSdk.Client;

namespace FireboltDotNetSdk.Tests.Integration
{
    [TestFixture]
    internal class RemoveParametersTest : IntegrationTest
    {
        [Test]
        public async Task RemoveParametersHeader_ServerInstructsParameterRemoval_ParametersRemovedFromClient()
        {
            await using var connection = new FireboltConnection(ConnectionString());
            await connection.OpenAsync();

            var client = connection.Client;

            var queryParamsField = typeof(FireboltClient).GetField("_queryParameters", BindingFlags.NonPublic | BindingFlags.Instance);
            var queryParams = (IDictionary<string, string>)queryParamsField!.GetValue(client)!;

            Assert.That(queryParams.ContainsKey("transaction_id"), Is.False, "Transaction ID should not be present initially");

            await using var command = connection.CreateCommand();
            command.CommandText = "BEGIN TRANSACTION;";

            await command.ExecuteNonQueryAsync();

            queryParamsField = typeof(FireboltClient).GetField("_queryParameters", BindingFlags.NonPublic | BindingFlags.Instance);
            queryParams = (IDictionary<string, string>)queryParamsField!.GetValue(client)!;

            Assert.That(queryParams["transaction_id"], Is.Not.Null);

            command.CommandText = "COMMIT;";

            await command.ExecuteNonQueryAsync();

            queryParamsField = typeof(FireboltClient).GetField("_queryParameters", BindingFlags.NonPublic | BindingFlags.Instance);
            queryParams = (IDictionary<string, string>)queryParamsField!.GetValue(client)!;

            Assert.That(queryParams.ContainsKey("transaction_id"), Is.False, "Transaction ID should be removed after commit");
        }
    }
}
