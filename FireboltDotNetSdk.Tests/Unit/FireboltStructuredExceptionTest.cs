using FireboltNETSDK.Exception;
using FireboltDotNetSdk.Utils;
using Newtonsoft.Json;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    public class FireboltStructuredExceptionTests
    {

        [Test]
        public void ParseSingleError_AllFieldsPopulated_ShouldMatchExpectedFormat()
        {
            var input = new[]
                {
                    new StructuredError
                    {
                        Code = "1001",
                        Name = "TestError",
                        Severity = "ERROR",
                        Source = "UnitTest",
                        Description = "This is a test error.",
                        Resolution = "Resolve it.",
                        HelpLink = "http://help.link",
                        Location = new Location { FailingLine = 42, StartOffset = 5, EndOffset = 10 }
                    }
                };

            var exception = new FireboltStructuredException(input.ToList());
            string expected = "ERROR: TestError (1001) - UnitTest, This is a test error., resolution: Resolve it. at FailingLine: 42, StartOffset: 5, EndOffset: 10, see http://help.link\n";
            Assert.That(exception.Message, Is.EqualTo(expected));
        }
        [Test]
        public void ParseMultipleErrors_AllFieldsPopulated_ShouldMatchExpectedFormat()
        {
            var input = new[]
                {
                    new StructuredError
                    {
                        Code = "2001",
                        Name = "FirstError",
                        Severity = "WARNING",
                        Source = "UnitTest1",
                        Description = "This is the first test error.",
                        Resolution = "Resolve it first.",
                        HelpLink = "http://help.first.link",
                        Location = new Location { FailingLine = 10, StartOffset = 1, EndOffset = 5 }
                    },
                    new StructuredError
                    {
                        Code = "2002",
                        Name = "SecondError",
                        Severity = "ERROR",
                        Source = "UnitTest2",
                        Description = "This is the second test error.",
                        Resolution = "Resolve it second.",
                        HelpLink = "http://help.second.link",
                        Location = new Location { FailingLine = 20, StartOffset = 2, EndOffset = 6 }
                    }
                };

            var exception = new FireboltStructuredException(input.ToList());
            string expected = "WARNING: FirstError (2001) - UnitTest1, This is the first test error., resolution: Resolve it first. at FailingLine: 10, StartOffset: 1, EndOffset: 5, see http://help.first.link\n" +
                              "ERROR: SecondError (2002) - UnitTest2, This is the second test error., resolution: Resolve it second. at FailingLine: 20, StartOffset: 2, EndOffset: 6, see http://help.second.link\n";
            Assert.That(exception.Message, Is.EqualTo(expected));
        }

        [Test]
        public void ParseError_MissingFields_ShouldHandleGracefully()
        {
            var input = new[]
            {
                new StructuredError
                {
                    Code = "3001",
                    Name = "MissingFieldsError",
                    Severity = "ERROR",
                    // Missing Source, Description, Resolution, HelpLink, Location
                }
            };

            var exception = new FireboltStructuredException(input.ToList());
            string expected = "ERROR: MissingFieldsError (3001)\n";
            Assert.That(exception.Message, Is.EqualTo(expected));
        }

        [Test]
        public void ParseError_OnlyDescription_HasNoLeadingSeparator()
        {
            var exception = new FireboltStructuredException(new List<StructuredError> { new() { Description = "Division by zero" } });
            Assert.That(exception.Message, Is.EqualTo("Division by zero\n"));
        }

        [Test]
        public void ParseError_EmptyLocation_IsLeftOut()
        {
            var exception = new FireboltStructuredException(new List<StructuredError>
            {
                new() { Description = "Division by zero", Location = new Location() }
            });
            Assert.That(exception.Message, Is.EqualTo("Division by zero\n"));
        }

        [TestCase("{\"failing_line\":1,\"start_offset\":9,\"end_offset\":12}")]
        [TestCase("{\"failingLine\":1,\"startOffset\":9,\"endOffset\":12}")]
        public void DeserializeLocation_EngineSnakeCaseAndCamelCase_AreBothRead(string json)
        {
            var location = JsonConvert.DeserializeObject<Location>(json)!;
            Assert.Multiple(() =>
            {
                Assert.That(location.FailingLine, Is.EqualTo(1));
                Assert.That(location.StartOffset, Is.EqualTo(9));
                Assert.That(location.EndOffset, Is.EqualTo(12));
            });
        }

        [Test]
        public void SerializeLocation_WritesEachFieldOnce()
        {
            var json = JsonConvert.SerializeObject(new Location { FailingLine = 1, StartOffset = 9, EndOffset = 12 });
            Assert.That(json, Is.EqualTo("{\"FailingLine\":1,\"StartOffset\":9,\"EndOffset\":12}"));
        }

        [Test]
        public void ParseEmptyList_ShouldReturnEmptyString()
        {
            var exception = new FireboltStructuredException(new List<StructuredError>());
            Assert.That(exception.Message, Is.EqualTo(""));
        }
    }
}