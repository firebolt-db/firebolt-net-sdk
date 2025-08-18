using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;

namespace FireboltDotNetSdk.Tests.Unit
{
    [TestFixture]
    public class FireboltTransactionTest
    {
        private const string ConnectionString = "database=testdb.ib;clientid=testuser;clientsecret=test_pwd;account=accountname;endpoint=api.mock.firebolt.io";

        [Test]
        public void BeginTransaction_ClosedConnection_ThrowsInvalidOperationException()
        {
            // Arrange
            var connection = new FireboltConnection(ConnectionString);
            // Connection is closed by default

            // Act & Assert
            var exception = Assert.Throws<FireboltException>(() => connection.BeginTransaction());
            Assert.Multiple(() =>
            {
                Assert.That(exception.Message, Does.Contain("Failed to begin transaction"));
                Assert.That(exception.InnerException, Is.Not.Null);
                Assert.That(exception.InnerException?.Message, Does.Contain("Client is undefined"));
            });
        }
    }
}
