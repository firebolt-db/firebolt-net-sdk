using System.Data.Common;
using System.Diagnostics;
using System.Net;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Utils;
using FireboltDotNetSdk.Exception;
using static FireboltDotNetSdk.Tests.Helpers.LargeParameterSet;
using static System.Environment;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    internal class LargeParameterSetTest : IntegrationTest
    {
        [TestCase(PreparedStatementParamStyleType.Native)]
        [TestCase(PreparedStatementParamStyleType.FbNumeric)]
        [Category("engine-v2")]
        public void ExecuteWithTenKbOfParameters(PreparedStatementParamStyleType style)
        {
            Assert.That(Uri.EscapeDataString(QueryParametersJson(TenKbParameterCount)).Length, Is.InRange(10 * 1024, 11 * 1024));
            using var connection = OpenConnection(style);

            AssertArrayResult(CreateArrayCommand(connection, style, TenKbParameterCount), TenKbParameterCount);
        }

        [Test]
        [Category("engine-v2")]
        public void ExecuteWithTwentyKbOfNativeParameters()
        {
            Assert.That(QueryParametersJson(TwentyKbParameterCount).Length, Is.InRange(20 * 1024, 21 * 1024));
            using var connection = OpenConnection(PreparedStatementParamStyleType.Native);

            AssertArrayResult(CreateArrayCommand(connection, PreparedStatementParamStyleType.Native, TwentyKbParameterCount), TwentyKbParameterCount);
        }

        // Gateway and runtime limits, not SDK behaviour, so CI does not run them: --filter Category=infra-limits
        [Test]
        [Category("infra-limits")]
        public void FbNumericWithTwentyKbOfParametersExceedsGatewayLimit()
        {
            if (GetEnvironmentVariable("FIREBOLT_CORE_URL") != null)
            {
                Assert.Ignore("Firebolt Core has no gateway; it accepts ~256 KiB of query_parameters");
            }
            using var connection = OpenConnection(PreparedStatementParamStyleType.FbNumeric);
            var command = CreateArrayCommand(connection, PreparedStatementParamStyleType.FbNumeric, TwentyKbParameterCount);

            var exception = Assert.Throws<FireboltException>(() => command.ExecuteReader());
            Assert.That(exception!.StatusCode, Is.EqualTo(HttpStatusCode.RequestUriTooLong));
        }

        [Test]
        [Category("engine-v2")]
        [Category("slow")]
        public void ExecuteWithCustomerSizedNativeParameterSet()
        {
            using var connection = OpenConnection(PreparedStatementParamStyleType.Native);

            AssertArrayResult(CreateArrayCommand(connection, PreparedStatementParamStyleType.Native, CustomerParameterCount), CustomerParameterCount);
        }

        // query_parameters travel in the URL, and System.Uri rejects URLs over ~64 KB before anything is sent.
        [Test]
        [Category("infra-limits")]
        public void FbNumericWithCustomerSizedParameterSetExceedsUriLimit()
        {
            using var connection = OpenConnection(PreparedStatementParamStyleType.FbNumeric);
            var command = CreateArrayCommand(connection, PreparedStatementParamStyleType.FbNumeric, CustomerParameterCount);

            Assert.Throws<UriFormatException>(() => command.ExecuteReader());
        }

        private static DbCommand CreateArrayCommand(DbConnection connection, PreparedStatementParamStyleType style, int count)
        {
            Func<int, string> placeholder = style == PreparedStatementParamStyleType.FbNumeric ? i => $"${i}" : i => $"@p{i}";
            var command = connection.CreateCommand();
            var placeholders = string.Join(", ", Enumerable.Range(1, count).Select(placeholder));
            command.CommandText = $"SELECT array_length(a), a[1], a[{count}] FROM (SELECT [{placeholders}] AS a)";
            for (int i = 1; i <= count; i++)
            {
                command.Parameters.Add(CreateParameter(command, placeholder(i), Value(i)));
            }
            return command;
        }

        private static void AssertArrayResult(DbCommand command, int count)
        {
            var stopwatch = Stopwatch.StartNew();
            using var reader = command.ExecuteReader();
            TestContext.Progress.WriteLine($"{TestContext.CurrentContext.Test.Name}: {count} parameters executed in {stopwatch.ElapsedMilliseconds} ms");
            Assert.Multiple(() =>
            {
                Assert.That(reader.Read(), Is.True);
                Assert.That(reader.GetInt32(0), Is.EqualTo(count));
                Assert.That(reader.GetString(1), Is.EqualTo(Value(1)));
                Assert.That(reader.GetString(2), Is.EqualTo(Value(count)));
                Assert.That(reader.Read(), Is.False);
            });
        }
    }
}
