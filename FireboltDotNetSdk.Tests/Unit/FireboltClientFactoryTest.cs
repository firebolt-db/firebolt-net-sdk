using System.Data.Common;
using System.Data;
using FireboltDotNetSdk.Client;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    public class FireboltClientFactoryTest
    {
        [Test]
        public void CreateEntityTest()
        {
            DbProviderFactory factory = FireboltClientFactory.Instance;
            Assert.NotNull(factory);
            Assert.False(factory.CanCreateDataSourceEnumerator);
            Assert.That(factory.CreateCommand()!.GetType(), Is.EqualTo(typeof(FireboltCommand)));
            Assert.That(factory.CreateCommandBuilder()!.GetType(), Is.EqualTo(typeof(FireboltCommandBuilder)));
            Assert.That(factory.CreateConnection()!.GetType(), Is.EqualTo(typeof(FireboltConnection)));
            Assert.That(factory.CreateConnectionStringBuilder()!.GetType(), Is.EqualTo(typeof(FireboltConnectionStringBuilder)));
            Assert.That(factory.CreateDataAdapter()!.GetType(), Is.EqualTo(typeof(FireboltDataAdapter)));
            Assert.That(factory.CreateParameter()!.GetType(), Is.EqualTo(typeof(FireboltParameter)));
        }

    }
}
