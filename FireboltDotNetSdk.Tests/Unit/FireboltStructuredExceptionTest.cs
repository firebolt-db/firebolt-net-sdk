using FireboltNETSDK.Exception;
using FireboltDotNetSdk.Utils;

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
        public void ParseEmptyList_ShouldReturnEmptyString()
        {
            var exception = new FireboltStructuredException(new List<StructuredError>());
            Assert.That(exception.Message, Is.EqualTo(""));
        }
    }
}