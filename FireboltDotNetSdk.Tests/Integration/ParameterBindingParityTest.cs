using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;

namespace FireboltDotNetSdk.Tests
{
    // Client-side (Native, @name) and server-side (FbNumeric, $n) binding must treat the same edge cases the same way.
    // A name that cannot be referenced fails in both: Native rejects it on the client, FbNumeric on the server.
    [TestFixture]
    internal class ParameterBindingParityTest : IntegrationTest
    {
        private static string Placeholder(PreparedStatementParamStyleType style, int index) =>
            style == PreparedStatementParamStyleType.Native ? $"@p{index}" : $"${index}";

        private static object?[] Execute(PreparedStatementParamStyleType style, string commandText, params (string Name, object? Value)[] parameters)
        {
            using var connection = OpenConnection(style);
            var command = connection.CreateCommand();
            command.CommandText = commandText;
            foreach (var (name, value) in parameters)
            {
                command.Parameters.Add(CreateParameter(command, name, value));
            }
            using var reader = command.ExecuteReader();
            Assert.That(reader.Read(), Is.True);
            var row = new object?[reader.FieldCount];
            reader.GetValues(row!);
            return row;
        }

        [TestCase(PreparedStatementParamStyleType.Native, false)]
        [TestCase(PreparedStatementParamStyleType.Native, true)]
        [TestCase(PreparedStatementParamStyleType.FbNumeric, false)]
        [TestCase(PreparedStatementParamStyleType.FbNumeric, true)]
        [Category("engine-v2")]
        public void ValueContainingAnotherPlaceholderIsNotReplacedAgain(PreparedStatementParamStyleType style, bool reverseOrder)
        {
            string p1 = Placeholder(style, 1), p2 = Placeholder(style, 2);
            var parameters = new[] { (p1, (object?)$"x{p2}"), (p2, "y") };
            if (reverseOrder)
            {
                Array.Reverse(parameters);
            }

            Assert.That(Execute(style, $"SELECT {p1}, {p2}", parameters), Is.EqualTo(new object[] { $"x{p2}", "y" }));
        }

        [TestCase(PreparedStatementParamStyleType.Native)]
        [TestCase(PreparedStatementParamStyleType.FbNumeric)]
        [Category("engine-v2")]
        public void ValueContainingItsOwnPlaceholderIsNotReplacedAgain(PreparedStatementParamStyleType style)
        {
            string p1 = Placeholder(style, 1);

            Assert.That(Execute(style, $"SELECT {p1}", (p1, p1)), Is.EqualTo(new object[] { p1 }));
        }

        [TestCase(PreparedStatementParamStyleType.Native)]
        [TestCase(PreparedStatementParamStyleType.FbNumeric)]
        [Category("engine-v2")]
        public void ParameterNameWithDotCannotBeReferenced(PreparedStatementParamStyleType style)
        {
            string dotted = Placeholder(style, 1) + ".b";

            Assert.That(() => Execute(style, $"SELECT {dotted}", (dotted, "x")), Throws.InstanceOf<FireboltException>());
        }

        [TestCase(PreparedStatementParamStyleType.Native)]
        [TestCase(PreparedStatementParamStyleType.FbNumeric)]
        [Category("engine-v2")]
        public void ParameterNameWithDotDoesNotBindItsPrefix(PreparedStatementParamStyleType style)
        {
            string p1 = Placeholder(style, 1);

            Assert.That(() => Execute(style, $"SELECT {p1}", (p1 + ".b", "x")), Throws.InstanceOf<FireboltException>());
        }
    }
}
