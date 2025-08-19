using System.Data;
using System.Reflection;
using System.Runtime.Serialization;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using Moq;
using Moq.Protected;

namespace FireboltDotNetSdk.Tests.Unit
{
    [TestFixture]
    public class FireboltTransactionTest
    {
        private const string ConnectionString = "database=testdb.ib;clientid=testuser;clientsecret=test_pwd;account=accountname;endpoint=api.mock.firebolt.io";
        private Mock<FireboltConnection> _mockConnection;
        private Mock<FireboltCommand> _mockCommand;

        [SetUp]
        public void Setup()
        {
            _mockConnection = new Mock<FireboltConnection>(ConnectionString);
            _mockCommand = new Mock<FireboltCommand>();

            _mockConnection.Protected().Setup<IDbCommand>("CreateDbCommand")
                .Returns(_mockCommand.Object);
            _mockCommand.SetupProperty(c => c.CommandText);
            _mockCommand.Setup(c => c.ExecuteNonQueryAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(1);
        }

        [Test]
        public void BeginTransactionClosedConnectionTest()
        {
            var connection = new FireboltConnection(ConnectionString);
            var exception = Assert.Throws<FireboltException>(() => connection.BeginTransaction());
            Assert.Multiple(() =>
            {
                Assert.That(exception.Message, Does.Contain("Failed to begin transaction"));
                Assert.That(exception.InnerException?.Message, Does.Contain("Client is undefined"));
            });
        }

        [Test]
        public void ConstructorWithNullConnectionThrowsArgumentNullExceptionTest()
        {
            var ex = Assert.Throws<ArgumentNullException>(() =>
                new FireboltTransaction(null!, IsolationLevel.ReadCommitted));
            Assert.That(ex.ParamName, Is.EqualTo("connection"));
        }

        [Test]
        public void ConstructorExecutesBeginTransactionTest()
        {
            var transaction = new FireboltTransaction(_mockConnection.Object, IsolationLevel.ReadCommitted);

            Assert.Multiple(() =>
            {
                _mockCommand.Verify(c => c.ExecuteNonQueryAsync(It.IsAny<CancellationToken>()), Times.Once);
                _mockCommand.VerifySet(c => c.CommandText = "BEGIN TRANSACTION");
                Assert.That(transaction.IsolationLevel, Is.EqualTo(IsolationLevel.ReadCommitted));
                Assert.That(transaction.Connection, Is.EqualTo(_mockConnection.Object));
            });
        }

        [Test]
        public void CommitExecutesCommitCommandTest()
        {
            var transaction = CreateMockTransaction();

            transaction.Commit();

            _mockCommand.Verify(c => c.ExecuteNonQueryAsync(It.IsAny<CancellationToken>()), Times.Once);
            _mockCommand.VerifySet(c => c.CommandText = "COMMIT");
        }

        [Test]
        public void CommitAsyncExecutesCommitCommandTest()
        {
            var transaction = CreateMockTransaction();

            transaction.CommitAsync().GetAwaiter().GetResult();

            _mockCommand.Verify(c => c.ExecuteNonQueryAsync(It.IsAny<CancellationToken>()), Times.Once);
            _mockCommand.VerifySet(c => c.CommandText = "COMMIT");
        }

        [Test]
        public void CommitAfterCommitThrowsFireboltExceptionTest()
        {
            var transaction = CreateMockTransaction();
            SetTransactionState(transaction, isCommitted: true);

            var ex = Assert.Throws<FireboltException>(() => transaction.Commit());
            Assert.That(ex.Message, Does.Contain("The transaction has already been committed"));
        }

        [Test]
        public void CommitAfterRollbackThrowsFireboltExceptionTest()
        {
            var transaction = CreateMockTransaction();
            SetTransactionState(transaction, isRolledBack: true);

            var ex = Assert.Throws<FireboltException>(() => transaction.Commit());
            Assert.That(ex.Message, Does.Contain("The transaction has already been rolled back"));
        }

        [Test]
        public void RollbackExecutesRollbackCommandTest()
        {
            var transaction = CreateMockTransaction();

            transaction.Rollback();

            _mockCommand.Verify(c => c.ExecuteNonQueryAsync(It.IsAny<CancellationToken>()), Times.Once);
            _mockCommand.VerifySet(c => c.CommandText = "ROLLBACK");
        }

        [Test]
        public void RollbackAsyncExecutesRollbackCommandTest()
        {
            var transaction = CreateMockTransaction();

            transaction.RollbackAsync().GetAwaiter().GetResult();

            _mockCommand.Verify(c => c.ExecuteNonQueryAsync(It.IsAny<CancellationToken>()), Times.Once);
            _mockCommand.VerifySet(c => c.CommandText = "ROLLBACK");
        }

        [Test]
        public void RollbackAfterCommitThrowsFireboltExceptionTest()
        {
            var transaction = CreateMockTransaction();
            SetTransactionState(transaction, isCommitted: true);

            var ex = Assert.Throws<FireboltException>(() => transaction.Rollback());
            Assert.That(ex.Message, Does.Contain("The transaction has already been committed"));
        }

        [Test]
        public void RollbackAfterRollbackThrowsFireboltExceptionTest()
        {
            var transaction = CreateMockTransaction();
            SetTransactionState(transaction, isRolledBack: true);

            var ex = Assert.Throws<FireboltException>(() => transaction.Rollback());
            Assert.That(ex.Message, Does.Contain("The transaction has already been rolled back"));
        }

        [Test]
        public void DisposeWithActiveTransactionExecutesRollbackTest()
        {
            var transaction = CreateMockTransaction();

            transaction.Dispose();

            _mockCommand.Verify(c => c.ExecuteNonQueryAsync(It.IsAny<CancellationToken>()), Times.Once);
            _mockCommand.VerifySet(c => c.CommandText = "ROLLBACK");
        }

        [Test]
        public void DisposeAfterCommitDoesNotExecuteRollbackTest()
        {
            var transaction = CreateMockTransaction();
            SetTransactionState(transaction, isCommitted: true);

            Assert.DoesNotThrow(() => transaction.Dispose());
            _mockCommand.Verify(c => c.ExecuteNonQueryAsync(It.IsAny<CancellationToken>()), Times.Never);
        }

        [Test]
        public void DisposeAfterRollbackDoesNotExecuteRollbackTest()
        {
            var transaction = CreateMockTransaction();
            SetTransactionState(transaction, isRolledBack: true);

            Assert.DoesNotThrow(() => transaction.Dispose());
            _mockCommand.Verify(c => c.ExecuteNonQueryAsync(It.IsAny<CancellationToken>()), Times.Never);
        }

        [Test]
        public void OperationsAfterDisposeThrowObjectDisposedExceptionTest()
        {
            var transaction = CreateMockTransaction();
            SetTransactionState(transaction, isDisposed: true);

            Assert.Multiple(() =>
            {
                Assert.Throws<ObjectDisposedException>(() => transaction.Commit());
                Assert.Throws<ObjectDisposedException>(() => transaction.Rollback());
                Assert.ThrowsAsync<ObjectDisposedException>(() => transaction.CommitAsync());
                Assert.ThrowsAsync<ObjectDisposedException>(() => transaction.RollbackAsync());
            });
        }

        [Test]
        public void ThrowIfCompletedCommittedVsRolledBackTest()
        {
            var transaction1 = CreateMockTransaction();
            SetTransactionState(transaction1, isCommitted: true);

            var ex1 = Assert.Throws<FireboltException>(() => transaction1.Commit());
            Assert.That(ex1.Message, Does.Contain("already been committed"));

            var transaction2 = CreateMockTransaction();
            SetTransactionState(transaction2, isRolledBack: true);

            var ex2 = Assert.Throws<FireboltException>(() => transaction2.Rollback());
            Assert.That(ex2.Message, Does.Contain("already been rolled back"));
        }

        [Test]
        public void ConnectionReturnsSuppliedConnectionTest()
        {
            var transaction = CreateMockTransaction();
            Assert.That(transaction.Connection, Is.SameAs(_mockConnection.Object));
        }

        [Test]
        public void CancellationTokenIsAcceptedByAsyncMethodsTest()
        {
            var transaction = CreateMockTransaction();
            var cancellationToken = CancellationToken.None;

            Assert.DoesNotThrow(() =>
            {
                _ = transaction.CommitAsync(cancellationToken);
                _ = transaction.RollbackAsync(cancellationToken);
            });
        }

        private FireboltTransaction CreateMockTransaction()
        {
            var transaction = (FireboltTransaction)FormatterServices.GetUninitializedObject(typeof(FireboltTransaction));

            var connectionField = typeof(FireboltTransaction).GetField("_dbConnection", BindingFlags.NonPublic | BindingFlags.Instance);
            var isDisposedField = typeof(FireboltTransaction).GetField("_isDisposed", BindingFlags.NonPublic | BindingFlags.Instance);

            connectionField?.SetValue(transaction, _mockConnection.Object);
            isDisposedField?.SetValue(transaction, false);

            return transaction;
        }

        private static void SetTransactionState(FireboltTransaction transaction, bool isCommitted = false, bool isRolledBack = false, bool isDisposed = false)
        {
            var type = typeof(FireboltTransaction);
            var isCommittedProperty = type.GetProperty("IsCommitted", BindingFlags.NonPublic | BindingFlags.Instance);
            var isRolledBackProperty = type.GetProperty("IsRolledBack", BindingFlags.NonPublic | BindingFlags.Instance);
            var isDisposedField = type.GetField("_isDisposed", BindingFlags.NonPublic | BindingFlags.Instance);

            isCommittedProperty?.SetValue(transaction, isCommitted);
            isRolledBackProperty?.SetValue(transaction, isRolledBack);
            isDisposedField?.SetValue(transaction, isDisposed);
        }
    }
}