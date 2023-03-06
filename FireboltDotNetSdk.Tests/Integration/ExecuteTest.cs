using System.Text;
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
            Assert.NotNull(value);
            Assert.IsNotEmpty(value!.Data);
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
            Assert.NotNull(value);
            Assert.IsNotEmpty(value!.Data);
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
            Assert.NotNull(value);
            Assert.IsNotEmpty(value!.Data);
        }

        [Test]
        public void ExecuteTestInvalidCredentials()
        {
            var connString = $"database={Database};username={Username};password=wrongPassword;endpoint={Endpoint};";
            using var conn = new FireboltConnection(connString);
            FireboltException? exception = Assert.Throws<FireboltException>(() => conn.Open());
            Assert.NotNull(exception);
            Assert.IsTrue(exception!.Message.Contains("The operation is forbidden\nStatus: 403") || exception.Message.Contains("429"));
            Assert.IsTrue(exception.ToString().Contains("FireboltDotNetSdk.Exception.FireboltException: The operation is forbidden\nStatus: 403") || exception.Message.Contains("429"));
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
            command.Execute("SELECT '2022-05-10 23:01:02.123455'::timestampntz");
            DateTime dt = new DateTime(2022, 5, 10, 23, 1, 2, 0).AddTicks(1234550);
            Assert.NotNull(command.Response);
            NewMeta newMeta = ResponseUtilities.getFirstRow(command.Response!);
            Assert.That(newMeta.Meta, Is.EqualTo("TimestampNtz"));
            Assert.That(newMeta.Data[0], Is.EqualTo(dt));
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
            DateTime dt = new DateTime(2022, 5, 10, 0, 0, 0);
            Assert.NotNull(command.Response);
            NewMeta newMeta = ResponseUtilities.getFirstRow(command.Response!);
            Assert.That(newMeta.Meta, Is.EqualTo("Date"));
            Assert.That(newMeta.Data[0], Is.EqualTo(DateOnly.FromDateTime(dt)));
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
            command.Execute("SELECT '2022-05-10 23:01:02.123456 Europe/Berlin'::timestamptz");

            DateTime dt = DateTime.Parse("2022-05-10 21:01:02.123456Z");
            Assert.NotNull(command.Response);
            NewMeta newMeta = ResponseUtilities.getFirstRow(command.Response!);
            Assert.That(newMeta.Data[0], Is.EqualTo(dt));
            Assert.That(newMeta.Meta, Is.EqualTo("TimestampTz"));

        }

        [Test]
        public void ExecuteSelectTimestampTzWithMinutesInTz()
        {
            var connString =
                $"database={Database};username={Username};password={Password};endpoint={Endpoint};account={Account}";
            using var conn = new FireboltConnection(connString);
            conn.Open();
            conn.SetEngine(Engine);
            var command = conn.CreateCursor();
            command.Execute("SET advanced_mode=1");
            command.Execute("SET time_zone=Asia/Calcutta");
            command.Execute("SELECT '2022-05-11 23:01:02.123123 Europe/Berlin'::timestamptz");

            DateTime dt = DateTime.Parse("2022-05-11 21:01:02.123123Z");
            Assert.NotNull(command.Response);
            NewMeta newMeta = ResponseUtilities.getFirstRow(command.Response!);
            Assert.That(newMeta.Data[0], Is.EqualTo(dt));
            Assert.That(newMeta.Meta, Is.EqualTo("TimestampTz"));
        }

        [Test]
        public void ExecuteSelectTimestampTzWithTzWithMinutesAndSecondsInTz()
        {
            var connString =
                $"database={Database};username={Username};password={Password};endpoint={Endpoint};account={Account}";
            using var conn = new FireboltConnection(connString);
            conn.Open();
            conn.SetEngine(Engine);
            var command = conn.CreateCursor();
            command.Execute("SET advanced_mode=1");
            command.Execute("SET time_zone=Asia/Calcutta");
            command.Execute("SELECT '1111-01-05 17:04:42.123456'::timestamptz");

            DateTime dt = DateTime.Parse("1111-01-05 11:11:14.123456Z");
            Assert.NotNull(command.Response);
            NewMeta newMeta = ResponseUtilities.getFirstRow(command.Response!);
            Assert.That(newMeta.Data[0], Is.EqualTo(dt));
            Assert.That(newMeta.Meta, Is.EqualTo("TimestampTz"));
        }

        [Test]
        public void ExecuteSelectTimestampTzWithTzWithDefaultTz()
        {
            var connString =
                $"database={Database};username={Username};password={Password};endpoint={Endpoint};account={Account}";
            using var conn = new FireboltConnection(connString);
            conn.Open();
            conn.SetEngine(Engine);
            var command = conn.CreateCursor();
            command.Execute("SELECT '2022-05-01 12:01:02.123456'::timestamptz");

            DateTime dt = DateTime.Parse("2022-05-01 12:01:02.123456Z");
            Assert.NotNull(command.Response);
            NewMeta newMeta = ResponseUtilities.getFirstRow(command.Response!);
            Assert.That(newMeta.Data[0], Is.EqualTo(dt));
            Assert.That(newMeta.Meta, Is.EqualTo("TimestampTz"));
        }

        [Test]
        public void ExecuteSelectBoolean()
        {
            var connString =
                $"database={Database};username={Username};password={Password};endpoint={Endpoint};account={Account}";

            using var conn = new FireboltConnection(connString);
            conn.Open();
            conn.SetEngine(Engine);
            var command = conn.CreateCursor();
            command.Execute("SET advanced_mode=1");
            command.Execute("SET output_format_firebolt_type_names=true");
            command.Execute("SET bool_output_format=postgres");
            command.Execute("SELECT true::boolean");
            Assert.NotNull(command.Response);
            NewMeta newMeta = ResponseUtilities.getFirstRow(command.Response!);
            Assert.That(newMeta.Meta, Is.EqualTo("Boolean"));
            Assert.That(newMeta.Data[0], Is.EqualTo(true));
        }

        [Test]
        public void ExecuteServiceAccountLogin()
        {
            var connString = $"database={Database};username={ClientId};password={ClientSecret};endpoint={Endpoint};account={Account}";
            using var conn = new FireboltConnection(connString);
            conn.Open();
            var command = conn.CreateCursor();
            var value = command.Execute("SELECT 1");
            Assert.NotNull(value);
            Assert.That(value!.Data[0][0], Is.EqualTo(1));
        }

        [Test]
        public void ExecuteServiceAccountLoginWithInvalidCredentials()
        {
            var connString = $"database={Database};username={ClientId};password=wrongPassword;endpoint={Endpoint};";
            using var conn = new FireboltConnection(connString);
            FireboltException? exception = Assert.Throws<FireboltException>(() => conn.Open());
            Assert.NotNull(exception);
            Assert.IsTrue(exception!.Message.Contains("401"));
        }
        [Test]
        public void ExecuteServiceAccountLoginWithMissingSecret()
        {
            var connString = $"database={Database};username={ClientId};password=;endpoint={Endpoint};";
            using var conn = new FireboltConnection(connString);
            FireboltException? exception = Assert.Throws<FireboltException>(() => conn.Open());
            Assert.NotNull(exception);
            Assert.IsTrue(exception!.Message.Contains("Password parameter is missing in the connection string"));
        }
        [Test]
        public void ExecuteSelectArray()
        {
            var connString = $"database={Database};username={ClientId};password={ClientSecret};endpoint={Endpoint};account={Account}";
            using var conn = new FireboltConnection(connString);
            conn.Open();
            var command = conn.CreateCursor();
            var res = command.Execute("select [1,NULL,3]");
            Assert.NotNull(command.Response);
            NewMeta newMeta = ResponseUtilities.getFirstRow(command.Response!);
            Assert.That(newMeta.Data[0], Is.EqualTo(new int?[] { 1, null, 3 }));
        }

        [Test]
        public void ExecuteSelectTwoDimensionalArray()
        {
            var connString = $"database={Database};username={ClientId};password={ClientSecret};endpoint={Endpoint};account={Account}";
            using var conn = new FireboltConnection(connString);
            conn.Open();
            var command = conn.CreateCursor();
            command.Execute("select [[1,NULL,3]]");
            int?[][] jaggedArray = new int?[1][];
            jaggedArray[0] = new int?[] { 1, null, 3 };
            Assert.NotNull(command.Response);
            NewMeta newMeta = ResponseUtilities.getFirstRow(command.Response!);
            Assert.That(newMeta.Data[0], Is.EqualTo(jaggedArray));
        }

        [Test]
        public void ExecuteSelectByteA()
        {
            var connString = $"database={Database};username={ClientId};password={ClientSecret};endpoint={Endpoint};account={Account}";
            using var conn = new FireboltConnection(connString);
            conn.Open();
            var command = conn.CreateCursor();
            command.Execute("SELECT 'hello_world_123ツ\n\u0048'::bytea");
            Assert.NotNull(command.Response == null);
            NewMeta newMeta = ResponseUtilities.getFirstRow(command.Response!);
            Assert.That(newMeta.Data[0], Is.EqualTo(Encoding.UTF8.GetBytes("hello_world_123ツ\n\u0048")));
        }
    }
}