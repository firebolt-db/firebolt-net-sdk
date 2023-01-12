using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;

namespace FireboltDotNetSdk.Tests
{

    [TestFixture]
    internal class ExecutionTest : IntegrationTest
    {
        [TestCase("SELECT 1")]
        [TestCase("SELECT 1, 'a'")]
        [TestCase("SELECT 1 as uint8")]
        [TestCase("SELECT 257 as uint16")]
        [TestCase("SELECT -257 as int16")]
        [TestCase("SELECT 80000 as uint32")]
        [TestCase("SELECT -80000 as int32")]
        [TestCase("SELECT 30000000000 as uint64")]
        [TestCase("SELECT -30000000000 as int64")]
        public void ExecuteTest(string commandText)
        {
            var connString = $"database={Database};username={Username};password={Password};endpoint={Endpoint}";

            using var conn = new FireboltConnection(connString);
            conn.Open();

            var cursor = conn.CreateCursor();

            var value = cursor.Execute(commandText);
            Assert.IsNotEmpty(value.Data);
        }

        [Ignore("sleepEachRow function is currently not supported on staging")]
        [TestCase("select sleepEachRow(1) from numbers(5)")]
        public void ExecuteSetTest(string commandText)
        {
            var connString = $"database={Database};username={Username};password={Password};endpoint={Endpoint};account={Account}";

            using var conn = new FireboltConnection(connString);
            conn.Open();

            var cursor = conn.CreateCursor();
            cursor.Execute("SET use_standard_sql=0");

            var value = cursor.Execute(commandText);
            Assert.IsNotEmpty(value.Data);
        }

        [TestCase("select * from information_schema.tables")]
        public void ExecuteSetEngineTest(string commandText)
        {
            var connString =
                $"database={Database};username={Username};password={Password};endpoint={Endpoint};account={Account}";

            using var conn = new FireboltConnection(connString);
            conn.Open();
            conn.SetEngine(Engine);

            var value = conn.CreateCursor().Execute(commandText);
            Assert.IsNotEmpty(value.Data);
        }

        [Test]
        public void ExecuteTestInvalidCredentials()
        {
            var connString = $"database={Database};username={Username};password=wrongPassword;endpoint={Endpoint};";
            using var conn = new FireboltConnection(connString);
            FireboltException exception = Assert.Throws<FireboltException>(() => conn.Open());
            Assert.IsTrue(exception.Message.Contains("403") || exception.Message.Contains("429"));
        }
        
        [Test]
        public void ExecuteSelectTimestampNtz()
        {
            var connString =
                $"database={Database};username={Username};password={Password};endpoint={Endpoint};account={Account}";

            using var conn = new FireboltConnection(connString);
            conn.Open();
            conn.SetEngine(Engine);
            var command = conn.CreateCursor();
            command.Execute("SELECT '2022-05-10 23:01:02.0'::timestampntz");
            DateTime dt = new DateTime(2022, 5, 10, 23, 1, 2, 0);
            NewMeta newMeta = getFirstRow(command.Response);
            Assert.AreEqual("TimestampNtz", newMeta.Meta);
            Assert.AreEqual(dt, newMeta.Data[0]);
        }
        
        [Test]
        public void ExecuteSelectTimestampPgDate()
        {
            var connString =
                $"database={Database};username={Username};password={Password};endpoint={Endpoint};account={Account}";

            using var conn = new FireboltConnection(connString);
            conn.Open();
            conn.SetEngine(Engine);
            var command = conn.CreateCursor();
            command.Execute("SELECT '2022-05-10'::pgdate");
            DateTime dt = new DateTime(2022, 5, 10,0,0, 0);
            NewMeta newMeta = getFirstRow(command.Response);
            Assert.AreEqual("Date", newMeta.Meta);
            Assert.AreEqual(DateOnly.FromDateTime(dt), newMeta.Data[0]);
        }
        
        [Test]
        public void ExecuteSelectTimestampTz()
        {
            var connString =
                $"database={Database};username={Username};password={Password};endpoint={Endpoint};account={Account}";

            using var conn = new FireboltConnection(connString);
            conn.Open();
            conn.SetEngine(Engine);
            var command = conn.CreateCursor();
            command.Execute("SELECT '2022-05-10 23:01:02.0 Europe/Berlin'::timestamptz");
           
            DateTime dt = new DateTime(2022, 5, 10, 22, 1, 2, 0);
            NewMeta newMeta = getFirstRow(command.Response);
            Assert.AreEqual(dt, newMeta.Data[0]);
            Assert.AreEqual("TimestampTz", newMeta.Meta);

        }

        private NewMeta getFirstRow(string response)
        {
            var enumerator = FireboltCommand.FormDataForResponse(response).GetEnumerator();
            enumerator.MoveNext();
            return enumerator.Current;
        }
    }
}