using System.Text;
using System.Data.Common;
using System.Collections;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using FireboltDoNetSdk.Utils;

namespace FireboltDotNetSdk.Tests
{

    [TestFixture]
    internal class ExecutionTest : IntegrationTest
    {
        private static string SYSTEM_CONNECTION_STRING = ConnectionStringWithout(nameof(Engine));
        private static string USER_CONNECTION_STRING = ConnectionString();
        private static string SYSTEM_WRONG_CREDENTIALS1 = ConnectionString(new Tuple<string, string?>(nameof(Engine), null), new Tuple<string, string?>(nameof(Password), "wrongPassword"));
        private static string SYSTEM_WRONG_CREDENTIALS2 = ConnectionString(new Tuple<string, string?>(nameof(Engine), null), new Tuple<string, string?>(nameof(ClientSecret), "wrongClientSecret"));
        private static string CREATE_SIMPLE_TABLE = @"
            CREATE FACT TABLE IF NOT EXISTS ALL_TYPES
            (
                i INTEGER  NULL,
                n NUMERIC NULL,
                bi BIGINT NULL,
                r REAL NULL,
                dec DECIMAL(8, 3) NULL,
                dp DOUBLE PRECISION NULL,
                t TEXT NULL,
                d DATE NULL,
                ts TIMESTAMP NULL,
                tstz TIMESTAMPTZ NULL,
                b BOOLEAN NULL, 
                ba BYTEA NULL
            )";

        private static string DROP_SIMPLE_TABLE = "DROP TABLE ALL_TYPES";
        private static string INSERT = "INSERT INTO ALL_TYPES (i, n, bi, r, dec, dp, t, d, ts, tstz, b, ba) VALUES (@i, @n, @bi, @r, @dec, @dp, @t, @d, @ts, @tstz, @b, @ba)";
        private static string SELECT_ALL_SIMPLE = "SELECT * FROM ALL_TYPES";
        private static string SELECT_WHERE_SIMPLE = "SELECT * FROM ALL_TYPES WHERE i = @i";

        [TestCase("SELECT 1")]
        [TestCase("SELECT 1, 'a'")]
        [TestCase("SELECT 1 as uint8")]
        [TestCase("SELECT 257 as uint16")]
        [TestCase("SELECT -257 as int16")]
        [TestCase("SELECT 80000 as uint32")]
        [TestCase("SELECT -80000 as int32")]
        [TestCase("SELECT 30000000000 as uint64")]
        [TestCase("SELECT -30000000000 as int64")]
        public void ExecuteSelectTest(string commandText)
        {
            executeTest(SYSTEM_CONNECTION_STRING, commandText, null, true);
        }

        [Ignore("sleepEachRow function is currently not supported on staging")]
        [TestCase("select sleepEachRow(1) from numbers(5)")]
        public void ExecuteSetTest(string commandText)
        {
            executeTest(SYSTEM_CONNECTION_STRING, commandText, "SET use_standard_sql=0", true);
        }

        [TestCase("select * from information_schema.tables")]
        public void ExecuteSetEngineTest(string commandText)
        {
            executeTest(USER_CONNECTION_STRING, commandText, null, false);
        }

        private void executeTest(string connString, string commandText, string? additionalCommandText, bool noNextLine)
        {
            using var conn = new FireboltConnection(connString);
            conn.Open();
            if (additionalCommandText != null)
            {
                CreateCommand(conn, additionalCommandText).ExecuteNonQuery();
            }
            DbCommand command = conn.CreateCommand();
            command.CommandText = commandText;
            DbDataReader reader = command.ExecuteReader();
            Assert.NotNull(reader);
            Assert.That(reader.Read(), Is.EqualTo(true));
            Assert.NotNull(reader.GetValue(0));
            if (noNextLine)
            {
                Assert.That(reader.Read(), Is.EqualTo(false));
            }
        }

        [TestCase("select 1", 1)]
        [TestCase("select 'hello'", "hello")]
        [TestCase("select 2, 'two'", 2)]
        [TestCase("select 'three', 3", "three")]
        [TestCase("SELECT 'one', 1 UNION ALL SELECT 'two', 2", "one")]
        public void ExecuteScalar(string query, object expectedValue)
        {
            using var conn = new FireboltConnection(USER_CONNECTION_STRING);
            conn.Open();
            DbCommand command = conn.CreateCommand();
            command.CommandText = query;
            Assert.That(command.ExecuteScalar(), Is.EqualTo(expectedValue));
        }

        [Test]
        [Category("v1")]
        public void ExecuteTestInvalidCredentialsV1()
        {
            ExecuteTestInvalidCredentials(SYSTEM_WRONG_CREDENTIALS1);
        }

        [Test]
        [Category("v2")]
        public void ExecuteTestInvalidCredentialsV2()
        {
            ExecuteTestInvalidCredentials(SYSTEM_WRONG_CREDENTIALS2);
        }

        private void ExecuteTestInvalidCredentials(string connectionString)
        {
            using var conn = new FireboltConnection(connectionString);
            FireboltException? exception = Assert.Throws<FireboltException>(() => conn.Open());
            Assert.NotNull(exception);
            Assert.That(exception!.Message, Does.Contain("The operation is unauthorized\nStatus: 401"));
            Assert.That(exception.ToString(), Does.Contain("FireboltDotNetSdk.Exception.FireboltException: The operation is unauthorized\nStatus: 401"));
        }

        [Test]
        public void ExecuteSelectTimestampNtz()
        {
            ExecuteDateTimeTest(
                USER_CONNECTION_STRING,
                "SELECT '2022-05-10 23:01:02.123455'::timestampntz",
                new string[0],
                new DateTime(2022, 5, 10, 23, 1, 2, 0).AddTicks(1234550));
        }

        [Test]
        public void ExecuteSelectTimestampPgDate()
        {
            ExecuteDateTimeTest(
                USER_CONNECTION_STRING,
                "SELECT '2022-05-10'::pgdate",
                new string[0],
                new DateTime(2022, 5, 10, 0, 0, 0),
                new DateTime(2022, 5, 10, 0, 0, 0));
        }

        [Test]
        public void ExecuteSelectTimestampTz()
        {
            using var conn = new FireboltConnection(USER_CONNECTION_STRING);
            conn.Open();
            ExecuteDateTimeTest(
                USER_CONNECTION_STRING,
                "SELECT '2022-05-10 23:01:02.123456 Europe/Berlin'::timestamptz",
                new string[0],
                DateTime.Parse("2022-05-10 21:01:02.123456Z"));
        }

        [Test]
        public void ExecuteSelectTimestampTzWithMinutesInTz()
        {
            ExecuteDateTimeTest(
                USER_CONNECTION_STRING,
                "SELECT '2022-05-11 23:01:02.123123 Europe/Berlin'::timestamptz",
                new string[] { "SET advanced_mode=1", "SET time_zone=Asia/Calcutta" },
                DateTime.Parse("2022-05-11 21:01:02.123123Z"));
        }

        [Test]
        public void ExecuteSelectTimestampTzWithTzWithMinutesAndSecondsInTz()
        {
            ExecuteDateTimeTest(
                USER_CONNECTION_STRING,
                "SELECT '1111-01-05 17:04:42.123456'::timestamptz",
                new string[] { "SET advanced_mode=1", "SET time_zone=Asia/Calcutta" },
                DateTime.Parse("1111-01-05 11:11:14.123456Z"));
        }

        [Test]
        public void ExecuteSelectTimestampTzWithTzWithDefaultTz()
        {
            ExecuteDateTimeTest(
                USER_CONNECTION_STRING,
                "SELECT '2022-05-01 12:01:02.123456'::timestamptz",
                new string[0],
                DateTime.Parse("2022-05-01 12:01:02.123456Z"));
        }

        private void ExecuteDateTimeTest(string connString, string commandText, string[] additionalCommands, DateTime expectedDateTime)
        {
            ExecuteDateTimeTest(connString, commandText, additionalCommands, expectedDateTime, expectedDateTime);
        }

        private void ExecuteDateTimeTest(string connString, string commandText, string[] additionalCommands, DateTime expectedDateTime, object expectedValue)
        {
            using var conn = new FireboltConnection(connString);
            conn.Open();
            foreach (string cmd in additionalCommands)
            {
                CreateCommand(conn, cmd).ExecuteNonQuery();
            }
            DbCommand command = conn.CreateCommand();
            command.CommandText = commandText;
            DbDataReader reader = command.ExecuteReader();
            Assert.NotNull(reader);
            Assert.That(reader.Read(), Is.EqualTo(true));
            Assert.That(reader.GetDateTime(0), Is.EqualTo(expectedDateTime));
            Assert.That(reader.GetValue(0), Is.EqualTo(expectedValue));
            Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(DateTime)));
            Assert.That(reader.Read(), Is.EqualTo(false));
            conn.Close();
        }


        [Test]
        public void ExecuteSelectBoolean()
        {
            using var conn = new FireboltConnection(USER_CONNECTION_STRING);
            conn.Open();

            DbCommand command = conn.CreateCommand();
            new List<string>()
            {
                "SET advanced_mode=1", "SET output_format_firebolt_type_names=true", "SET bool_output_format=postgres"
            }
            .ForEach(set => { command.CommandText = set; command.ExecuteNonQuery(); });
            command.CommandText = "SELECT true::boolean";
            DbDataReader reader = command.ExecuteReader();
            Assert.IsTrue(reader.Read());
            Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(bool)));
            Assert.That(reader.GetBoolean(0), Is.EqualTo(true));
            Assert.IsFalse(reader.Read());
        }

        [Test]
        public void ExecuteServiceAccountLogin()
        {
            using var conn = new FireboltConnection(SYSTEM_CONNECTION_STRING);
            conn.Open();
            DbCommand command = conn.CreateCommand();
            command.CommandText = "SELECT 1";
            DbDataReader reader = command.ExecuteReader();
            Assert.That(reader.Read(), Is.EqualTo(true));
            Assert.That(reader.GetInt16(0), Is.EqualTo(1));
            Assert.That(reader.Read(), Is.EqualTo(false));
            conn.Close();
        }

        [Test]
        public void ExecuteInformationSchema()
        {
            using var conn = new FireboltConnection(SYSTEM_CONNECTION_STRING);
            conn.Open();
            DbCommand command = conn.CreateCommand();
            command.CommandText = "SELECT table_name, number_of_rows from information_schema.tables";
            DbDataReader reader = command.ExecuteReader();
            List<string> tableNames = new List<string>();
            while (reader.Read())
            {
                string name = reader.GetString(0);
                tableNames.Add(name);
                long rowsCount = reader.GetInt64(1);
                Assert.That(rowsCount, Is.GreaterThanOrEqualTo(0));
            }
            List<string> expectedTables = new List<string>() { "databases", "engines", "tables", "views", "columns", "accounts", "users" };
            expectedTables.ForEach(table => Assert.That(tableNames.Contains(table), Is.EqualTo(true)));
        }

        [Test]
        [Category("v1")]
        public void ExecuteServiceAccountLoginWithInvalidCredentialsV1()
        {
            ExecuteServiceAccountLoginWithInvalidCredentials(SYSTEM_WRONG_CREDENTIALS1);
        }

        [Test]
        [Category("v2")]
        public void ExecuteServiceAccountLoginWithInvalidCredentialsV2()
        {
            ExecuteServiceAccountLoginWithInvalidCredentials(SYSTEM_WRONG_CREDENTIALS2);
        }

        private void ExecuteServiceAccountLoginWithInvalidCredentials(string connectionString)
        {
            using var conn = new FireboltConnection(connectionString);
            FireboltException? exception = Assert.Throws<FireboltException>(() => conn.Open());
            Assert.NotNull(exception);
            Assert.IsTrue(exception!.Message.Contains("401"));
        }

        [TestCase(nameof(Password), Category = "v1")]
        [TestCase(nameof(ClientSecret), Category = "v2")]
        public void ExecuteServiceAccountLoginWithMissingSecret(string secretField)
        {
            var connString = ConnectionString(new Tuple<string, string?>(nameof(Engine), null), new Tuple<string, string?>(secretField, ""));
            FireboltException? exception = Assert.Throws<FireboltException>(() => new FireboltConnection(connString));
            Assert.NotNull(exception);
            Assert.That(exception!.Message, Is.EqualTo("Configuration error: either Password or ClientSecret must be provided but not both"));
        }

        [Test]
        public void ExecuteSelectArray()
        {
            ExecuteTest(SYSTEM_CONNECTION_STRING, "select [1,NULL,3]", new int?[] { 1, null, 3 }, typeof(int[]));
        }

        [Test]
        public void ExecuteSelectTwoDimensionalArray()
        {
            ExecuteTest(SYSTEM_CONNECTION_STRING, "select [[1,NULL,3]]", new int?[1][] { new int?[] { 1, null, 3 } }, typeof(int[][]));
        }

        private void ExecuteTest(string connString, string commandText, object expectedValue, Type expectedType)
        {
            var conn = new FireboltConnection(connString);
            conn.Open();
            DbDataReader reader = ExecuteTest(conn, commandText, expectedValue, null); //TODO: improve type discovery and send expectedType here
            Assert.That(reader.Read(), Is.EqualTo(false));
            conn.Close();
        }

        private DbDataReader ExecuteTest(DbConnection conn, string commandText, object expectedValue, Type? expectedType)
        {
            DbCommand command = conn.CreateCommand();
            command.CommandText = commandText;
            DbDataReader reader = command.ExecuteReader();
            Assert.NotNull(reader);
            Assert.That(reader.Read(), Is.EqualTo(true));
            Assert.That(reader.GetValue(0), Is.EqualTo(expectedValue));
            if (expectedType != null)
            {
                Assert.That(reader.GetFieldType(0), Is.EqualTo(expectedType));
            }
            return reader;
        }

        [Test]
        public void ExecuteSelectByteA()
        {
            ExecuteTest(SYSTEM_CONNECTION_STRING, "SELECT 'hello_world_123ツ\n\u0048'::bytea", Encoding.UTF8.GetBytes("hello_world_123ツ\n\u0048"), typeof(byte[]));
        }

        [Test]
        public void ShouldThrowExceptionWhenEngineIsNotFound()
        {
            var connString = $"{SYSTEM_CONNECTION_STRING};engine=InexistantEngine";
            using var conn = new FireboltConnection(connString);
            FireboltException? exception = Assert.Throws<FireboltException>(() => conn.Open());
            Assert.That(exception!.Message, Is.EqualTo($"Engine InexistantEngine not found."));
        }

        [Test]
        public void Select1()
        {
            ExecuteTest(USER_CONNECTION_STRING, "SELECT 1", 1, typeof(int));
        }

        [Test]
        public void WrongQuerySystemEngine()
        {
            WrongQuery(SYSTEM_CONNECTION_STRING);
        }

        [Test]
        public void WrongQueryUserEngine()
        {
            WrongQuery(USER_CONNECTION_STRING);
        }

        private void WrongQuery(string connString)
        {
            using (var conn = new FireboltConnection(connString))
            {
                conn.Open();
                DbCommand command = conn.CreateCommand();
                command.CommandText = "wrong query";
                Assert.Throws<FireboltException>(() => command.ExecuteReader());
                Assert.Throws<FireboltException>(() => command.ExecuteNonQuery());
            }
        }

        [TestCase("SELECT 1/0", double.PositiveInfinity, float.PositiveInfinity)]
        [TestCase("SELECT 1/0  as int16", double.PositiveInfinity, float.PositiveInfinity)]
        [TestCase("SELECT -1/0", double.NegativeInfinity, float.NegativeInfinity)]
        public void SelectInfinity(string query, double doubleExpected, float floatExpected)
        {
            var conn = new FireboltConnection(USER_CONNECTION_STRING);
            conn.Open();
            DbDataReader reader = ExecuteTest(conn, query, doubleExpected, typeof(double));
            Assert.That(reader.GetDouble(0), Is.EqualTo(doubleExpected));
            Assert.That(reader.GetFloat(0), Is.EqualTo(floatExpected));
            Assert.Throws<InvalidCastException>(() => reader.GetInt16(0));
            Assert.Throws<InvalidCastException>(() => reader.GetInt32(0));
            Assert.Throws<InvalidCastException>(() => reader.GetInt64(0));
            Assert.That(reader.Read(), Is.EqualTo(false));
            conn.Close();
        }

        [Test]
        public void CreateDropFillTableWithPrimitives()
        {
            byte[] bytes = Encoding.UTF8.GetBytes("hello.привет.שלום");
            using (var conn = new FireboltConnection(USER_CONNECTION_STRING))
            {
                conn.Open();
                CreateCommand(conn, CREATE_SIMPLE_TABLE).ExecuteNonQuery();
                DbCommand insert = CreateCommand(conn, INSERT);
                insert.Prepare();
                insert.Parameters.Add(CreateParameter(insert, "@i", 1));
                insert.Parameters.Add(CreateParameter(insert, "@n", 2));
                insert.Parameters.Add(CreateParameter(insert, "@bi", 1234567890L));
                insert.Parameters.Add(CreateParameter(insert, "@r", 3.14));
                insert.Parameters.Add(CreateParameter(insert, "@dec", 6.02));
                insert.Parameters.Add(CreateParameter(insert, "@dp", 2.718281828));
                insert.Parameters.Add(CreateParameter(insert, "@t", "hello"));
                insert.Parameters.Add(CreateParameter(insert, "@d", "2023-10-18"));
                insert.Parameters.Add(CreateParameter(insert, "@ts", "2023-10-18 17:30:20.123456"));
                insert.Parameters.Add(CreateParameter(insert, "@tstz", "2023-10-18 17:30:20.123456+02"));
                insert.Parameters.Add(CreateParameter(insert, "@b", true));
                insert.Parameters.Add(CreateParameter(insert, "@ba", bytes));
                insert.ExecuteNonQuery();

                DbCommand selectAll = CreateCommand(conn, SELECT_ALL_SIMPLE);
                Type[] expectedTypes = new Type[]
                {
                    typeof(int), typeof(decimal), typeof(long), typeof(float), typeof(decimal), typeof(double),
                    typeof(string), typeof(DateTime), typeof(DateTime), typeof(DateTime), typeof(bool),
                    typeof(byte[])
                };

                using (DbDataReader reader = selectAll.ExecuteReader())
                {
                    int n = reader.FieldCount;
                    for (int i = 0; i < n; i++)
                    {
                        Assert.That(
                            reader.GetFieldType(i),
                            Is.EqualTo(expectedTypes[i]),
                            $"Wrong data type of column {i}: expected {expectedTypes[i]} but was {reader.GetFieldType(i)}");
                    }

                    while (reader.Read())
                    {
                        Assert.That(reader.GetInt32(0), Is.EqualTo(1));
                        Assert.That(reader.GetDecimal(1), Is.EqualTo(2));
                        Assert.That(reader.GetInt64(2), Is.EqualTo(1234567890L));
                        Assert.True(Math.Abs(reader.GetFloat(3) - 3.14) <= 0.001);
                        Assert.True(Math.Abs(reader.GetDecimal(4) - 6.02M) <= 0.001M);
                        Assert.True(Math.Abs(reader.GetDouble(5) - 2.718281828) <= 0.001);
                        Assert.That(reader.GetString(6), Is.EqualTo("hello"));
                        Assert.That(reader.GetDateTime(7), Is.EqualTo(TypesConverter.ParseDateTime("2023-10-18")));
                        Assert.That(reader.GetDateTime(8), Is.EqualTo(TypesConverter.ParseDateTime("2023-10-18 17:30:20.123456")));
                        Assert.That(reader.GetDateTime(9), Is.EqualTo(TypesConverter.ParseDateTime("2023-10-18 17:30:20.123456+02")));
                        Assert.That(reader.GetBoolean(10), Is.EqualTo(true));
                        // byte array
                        Assert.That(reader.GetValue(11), Is.EqualTo(bytes));
                        byte[] buffer = new byte[bytes.Length];
                        long length = reader.GetBytes(11, 0, buffer, 0, buffer.Length);
                        Assert.That(length, Is.EqualTo(buffer.Length));
                        Assert.That(buffer, Is.EqualTo(bytes));
                    }
                }

                using (DbDataReader reader = selectAll.ExecuteReader())
                {
                    IEnumerator e = reader.GetEnumerator();
                    while (e.MoveNext())
                    {
                        DbDataRecord record = (DbDataRecord)e.Current;

                        int n = record.FieldCount;
                        for (int i = 0; i < n; i++)
                        {
                            Assert.That(
                                record.GetFieldType(i),
                                Is.EqualTo(expectedTypes[i]),
                                $"Wrong data type of column {i}: expected {expectedTypes[i]} but was {record.GetFieldType(i)}");
                        }

                        Assert.That(record.GetInt32(0), Is.EqualTo(1));
                        Assert.That(record.GetDecimal(1), Is.EqualTo(2));
                        Assert.That(record.GetInt64(2), Is.EqualTo(1234567890L));
                        Assert.True(Math.Abs(record.GetFloat(3) - 3.14) <= 0.001);
                        Assert.True(Math.Abs(record.GetDecimal(4) - 6.02M) <= 0.001M);
                        Assert.True(Math.Abs(record.GetDouble(5) - 2.718281828) <= 0.001);
                        Assert.That(record.GetString(6), Is.EqualTo("hello"));
                        Assert.That(record.GetDateTime(7), Is.EqualTo(TypesConverter.ParseDateTime("2023-10-18")));
                        Assert.That(record.GetDateTime(8), Is.EqualTo(TypesConverter.ParseDateTime("2023-10-18 17:30:20.123456")));
                        Assert.That(record.GetDateTime(9), Is.EqualTo(TypesConverter.ParseDateTime("2023-10-18 17:30:20.123456+02")));
                        Assert.That(record.GetBoolean(10), Is.EqualTo(true));
                        // byte array
                        Assert.That(record.GetValue(11), Is.EqualTo(bytes));
                        byte[] buffer = new byte[bytes.Length];
                        long length = record.GetBytes(11, 0, buffer, 0, buffer.Length);
                        Assert.That(length, Is.EqualTo(buffer.Length));
                        Assert.That(buffer, Is.EqualTo(bytes));
                    }
                }

                Assert.True(selectOneRow(conn, 1));
                Assert.False(selectOneRow(conn, 0));

                CreateCommand(conn, DROP_SIMPLE_TABLE).ExecuteNonQuery();
            }
        }

        [Test]
        public void CreateDropFillTableWithEmptyArrays()
        {
            CreateDropFillTableWithArrays("int", new int[0], typeof(int[]), "int", new int[0][], typeof(int[][]), "int", new int[0][][], typeof(int[][][]));
        }

        [Test]
        public void CreateDropFillTableWithNullArrays()
        {
            CreateDropFillTableWithArrays("int", null, typeof(int[]), "int", null, typeof(int[][]), "int", null, typeof(int[][][]));
        }

        [Test]
        public void CreateDropFillTableWithSingetonArrays()
        {
            CreateDropFillTableWithArrays(
                "int", new int[] { 1 }, typeof(int[]),
                "int", new int[][] { new int[] { 22 } }, typeof(int[][]),
                "int", new int[][][] { new int[][] { new int[] { 333 } } }, typeof(int[][][]));
        }

        [Test]
        public void CreateDropFillTableWithFullArrays()
        {
            CreateDropFillTableWithArrays(
                "int", new int[] { 1, 2, 3 }, typeof(int[]),
                "int", new int[][] { new int[] { 21, 22, 23 }, new int[] { 31, 32, 33 } }, typeof(int[][]),
                "int", new int[][][] { new int[][] { new int[] { 333, 334, 335, 336 }, new int[] { 1, 2 } } }, typeof(int[][][])
            );
        }

        [Test]
        public void CreateDropFillTableWithArraysOfDifferentTypes()
        {
            CreateDropFillTableWithArrays(
                "long", new long[] { 1, 2 }, typeof(long[]),
                "float", new float[][] { new float[] { 2.7F, 3.14F } }, typeof(float[][]),
                "double", new double[][][] { new double[][] { new double[] { 3.1415926 } } }, typeof(double[][][]));
        }

        private void CreateDropFillTableWithArrays(string type1, Array? inta1, Type expType1, string type2, Array? inta2, Type expType2, string type3, Array? inta3, Type expType3)
        {
            using (var conn = new FireboltConnection(USER_CONNECTION_STRING))
            {
                conn.Open();
                string CREATE_ARRAYS_TABLE = @$"
                    CREATE FACT TABLE ALL_ARRAYS
                    (
                        i INTEGER  NULL,
                        inta1 {type1}[] NULL,
                        inta2 {type2}[][] NULL,
                        inta3 {type3}[][][] NULL
                    )
                ";
                try
                {
                    CreateCommand(conn, CREATE_ARRAYS_TABLE).ExecuteNonQuery();
                    string INSERT_ARRAYS = "INSERT INTO ALL_ARRAYS (i, inta1, inta2, inta3) VALUES (@i, @inta1, @inta2, @inta3)";
                    int id = 1;
                    DbCommand insert = CreateCommand(conn, INSERT_ARRAYS);
                    insert.Prepare();
                    insert.Parameters.Add(CreateParameter(insert, "@i", id));
                    insert.Parameters.Add(CreateParameter(insert, "@inta1", inta1));
                    insert.Parameters.Add(CreateParameter(insert, "@inta2", inta2));
                    insert.Parameters.Add(CreateParameter(insert, "@inta3", inta3));
                    insert.ExecuteNonQuery();

                    string SELECT_ARRAYS_WHERE = "SELECT i, inta1, inta2, inta3 FROM ALL_ARRAYS WHERE i = @i";
                    DbCommand select = CreateCommand(conn, SELECT_ARRAYS_WHERE);
                    select.Parameters.Add(CreateParameter(select, "@i", id));
                    Type[] expectedTypes = new Type[] { typeof(int), expType1, expType2, expType3 };

                    using (DbDataReader reader = select.ExecuteReader())
                    {
                        int n = reader.FieldCount;
                        for (int i = 0; i < n; i++)
                        {
                            Assert.That(
                                reader.GetFieldType(i),
                                Is.EqualTo(expectedTypes[i]),
                                $"Wrong data type of column {i}: expected {expectedTypes[i]} but was {reader.GetFieldType(i)}");
                        }
                        int rowsCount = 0;
                        while (reader.Read())
                        {
                            Assert.That(reader.GetInt32(0), Is.EqualTo(id));
                            Assert.That(reader.GetValue(1), Is.EqualTo(inta1 == null ? DBNull.Value : inta1));
                            Assert.That(reader.GetValue(2), Is.EqualTo(inta2 == null ? DBNull.Value : inta2));
                            Assert.That(reader.GetValue(3), Is.EqualTo(inta3 == null ? DBNull.Value : inta3));
                            rowsCount++;
                        }
                        Assert.That(rowsCount, Is.EqualTo(1));
                    }

                    using (DbDataReader reader = select.ExecuteReader())
                    {
                        int rowsCount = 0;
                        IEnumerator e = reader.GetEnumerator();
                        while (e.MoveNext())
                        {
                            DbDataRecord record = (DbDataRecord)e.Current;

                            int n = record.FieldCount;
                            for (int i = 0; i < n; i++)
                            {
                                Assert.That(
                                    record.GetFieldType(i),
                                    Is.EqualTo(expectedTypes[i]),
                                    $"Wrong data type of column {i}: expected {expectedTypes[i]} but was {record.GetFieldType(i)}");
                            }

                            Assert.That(record.GetInt32(0), Is.EqualTo(id));
                            Assert.That(record.GetInt32(0), Is.EqualTo(1));
                            Assert.That(record.GetValue(1), Is.EqualTo(inta1 == null ? DBNull.Value : inta1));
                            Assert.That(record.GetValue(2), Is.EqualTo(inta2 == null ? DBNull.Value : inta2));
                            Assert.That(record.GetValue(3), Is.EqualTo(inta3 == null ? DBNull.Value : inta3));
                            rowsCount++;
                        }
                        Assert.That(rowsCount, Is.EqualTo(1));
                    }
                }
                finally
                {
                    CreateCommand(conn, "DROP TABLE ALL_ARRAYS").ExecuteNonQuery();
                }
            }
        }

        // Executes select ... where i = ? and returns true if at least one record is found and false otherwise. 
        private bool selectOneRow(DbConnection conn, int id)
        {
            DbCommand selectWhere1 = CreateCommand(conn, SELECT_WHERE_SIMPLE);
            selectWhere1.Prepare();
            selectWhere1.Parameters.Add(CreateParameter(selectWhere1, "@i", id));
            using (DbDataReader reader = selectWhere1.ExecuteReader())
            {
                bool found = false;
                while (reader.Read())
                {
                    Assert.That(reader.GetInt32(0), Is.EqualTo(1));
                    found = true;
                }
                return found;
            }
        }

        private DbCommand CreateCommand(DbConnection conn, string query)
        {
            DbCommand command = conn.CreateCommand();
            command.CommandText = query;
            return command;
        }

        private DbParameter CreateParameter(DbCommand command, string name, object? value)
        {
            DbParameter parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value;
            return parameter;
        }
    }
}
