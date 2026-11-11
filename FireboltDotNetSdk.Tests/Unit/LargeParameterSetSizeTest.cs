using static FireboltDotNetSdk.Tests.Helpers.LargeParameterSet;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    public class LargeParameterSetSizeTest
    {
        [Test]
        public void TenKbParameterSetIsTenKbOnceUrlEscaped()
        {
            Assert.That(Uri.EscapeDataString(QueryParametersJson(TenKbParameterCount)).Length, Is.InRange(10 * 1024, 11 * 1024));
        }
    }
}
