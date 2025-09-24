using System.Data.Common;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Utils;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    [Category("v2")]
    [Category("engine-v2")]
    internal class StructMappingTest : IntegrationTest
    {
        private const string TableName = "struct_test";

        private static readonly string[] SetupSql =
        {
            "SET advanced_mode=1",
            "SET enable_create_table_v2=true",
            "SET enable_struct_syntax=true",
            "SET prevent_create_on_information_schema=true",
            "SET enable_create_table_with_struct_type=true",
            $"DROP TABLE IF EXISTS {TableName}",
            $"CREATE TABLE IF NOT EXISTS {TableName} (" +
            "id numeric(5,0), " +
            "plain struct(int_val int, str_val text, arr_val array(numeric(5,3)))" +
            ")"
        };

        private const string CleanupSql = $"DROP TABLE IF EXISTS {TableName}";

        [OneTimeSetUp]
        public void GlobalSetup()
        {
            using var conn = new FireboltConnection(ConnectionString());
            conn.Open();
            SetupStructTable(conn);
        }

        [OneTimeTearDown]
        public void GlobalTearDown()
        {
            using var conn = new FireboltConnection(ConnectionString());
            conn.Open();
            CreateCommand(conn, CleanupSql).ExecuteNonQuery();
        }

        [Test]
        public void InsertAndSelectStruct_PocoWithAttributesMapping_Sync()
        {
            using var conn = new FireboltConnection(ConnectionString());
            conn.Open();

            var select = CreateCommand(conn, $"SELECT id, plain FROM {TableName} ORDER BY id ASC");
            using var reader = select.ExecuteReader();
            Assert.Multiple(() =>
            {
                Assert.That(reader.Read(), Is.True);
                Assert.That(reader.GetFieldValue<decimal>(0), Is.EqualTo(1));
            });
            var plain = reader.GetFieldValue<PlainStructWithAttributes>(1);
            AssertPocoWithAttributes(plain);
            Assert.That(reader.Read(), Is.False);
        }

        [Test]
        public async Task InsertAndSelectStruct_PocoWithAttributesMapping_Async()
        {
            await using var conn = new FireboltConnection(ConnectionString());
            await conn.OpenAsync();

            var select = CreateCommand(conn, $"SELECT id, plain FROM {TableName} ORDER BY id ASC");
            await using var reader = await select.ExecuteReaderAsync();
            Assert.Multiple(async () =>
            {
                Assert.That(await reader.ReadAsync(), Is.True);
                Assert.That(await reader.GetFieldValueAsync<decimal>(0), Is.EqualTo(1));
            });
            var plain = await reader.GetFieldValueAsync<PlainStructWithAttributes>(1);
            AssertPocoWithAttributes(plain);
            Assert.That(await reader.ReadAsync(), Is.False);
        }

        [Test]
        public void InsertAndSelectStruct_PocoWithoutAttributesMapping_Sync()
        {
            using var conn = new FireboltConnection(ConnectionString());
            conn.Open();

            var select = CreateCommand(conn, $"SELECT id, plain FROM {TableName} ORDER BY id ASC");
            using var reader = select.ExecuteReader();
            Assert.Multiple(() =>
            {
                Assert.That(reader.Read(), Is.True);
                Assert.That(reader.GetFieldValue<decimal>(0), Is.EqualTo(1));
            });
            var plain = reader.GetFieldValue<PlainStructWithoutAttributes>(1);
            AssertPocoWithoutAttributes(plain);
            Assert.That(reader.Read(), Is.False);
        }

        [Test]
        public async Task InsertAndSelectStruct_PocoWithoutAttributesMapping_Async()
        {
            await using var conn = new FireboltConnection(ConnectionString());
            await conn.OpenAsync();

            var select = CreateCommand(conn, $"SELECT id, plain FROM {TableName} ORDER BY id ASC");
            await using var reader = await select.ExecuteReaderAsync();
            Assert.Multiple(async () =>
            {
                Assert.That(await reader.ReadAsync(), Is.True);
                Assert.That(await reader.GetFieldValueAsync<decimal>(0), Is.EqualTo(1));
            });
            var plain = await reader.GetFieldValueAsync<PlainStructWithoutAttributes>(1);
            AssertPocoWithoutAttributes(plain);
            Assert.That(await reader.ReadAsync(), Is.False);
        }

        private static void AssertPocoWithAttributes(PlainStructWithAttributes plain)
        {
            Assert.Multiple(() =>
            {
                Assert.That(plain.IntVal, Is.EqualTo(1));
                Assert.That(plain.StrVal, Is.EqualTo("test"));
                Assert.That(plain.ArrVal, Is.Not.Null);
                Assert.That(plain.ArrVal, Has.Length.EqualTo(2));
                Assert.That(plain.ArrVal[0], Is.EqualTo(12.34f).Within(1e-4));
                Assert.That(plain.ArrVal[1], Is.EqualTo(56.789f).Within(1e-4));
            });
        }

        private static void AssertPocoWithoutAttributes(PlainStructWithoutAttributes plain)
        {
            Assert.Multiple(() =>
            {
                Assert.That(plain.IntVal, Is.EqualTo(1));
                Assert.That(plain.StrVal, Is.EqualTo("test"));
                Assert.That(plain.ArrVal, Is.Not.Null);
                Assert.That(plain.ArrVal, Has.Length.EqualTo(2));
                Assert.That(plain.ArrVal[0], Is.EqualTo(12.34f).Within(1e-4));
                Assert.That(plain.ArrVal[1], Is.EqualTo(56.789f).Within(1e-4));
            });
        }

        private static void SetupStructTable(FireboltConnection conn)
        {
            foreach (var sql in SetupSql)
            {
                CreateCommand(conn, sql).ExecuteNonQuery();
            }

            // Insert a struct row
            const string insert = $"INSERT INTO {TableName} (id, plain) VALUES (1, struct(1, 'test', [12.34, 56.789]))";
            CreateCommand(conn, insert).ExecuteNonQuery();
        }

        private class PlainStructWithAttributes
        {

            [FireboltStructName("int_val")]
            public int IntVal { get; init; }

            [FireboltStructName("str_val")]
            public string StrVal { get; init; } = null!;

            [FireboltStructName("arr_val")]
            public float[] ArrVal { get; init; } = null!;
        }

        private class PlainStructWithoutAttributes
        {

            public int IntVal { get; init; }

            public string StrVal { get; init; } = null!;

            public float[] ArrVal { get; init; } = null!;
        }

        private static DbCommand CreateCommand(DbConnection conn, string query)
        {
            var command = conn.CreateCommand();
            command.CommandText = query;
            return command;
        }
    }
}


