using System.Text;
using System.Data;
using System.Data.Common;
using System.Collections;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using FireboltDoNetSdk.Utils;
using NodaTime.Extensions;
using FireboltNETSDK.Exception;

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
        private static string LONG_QUERY = "SELECT checksum(*) FROM GENERATE_SERIES(1, 300000000000)";

        [TestCase("SELECT 1")]
        [TestCase("SELECT 1, 'a'")]
        [TestCase("SELECT 1 as uint8")]
        [TestCase("SELECT 257 as uint16")]
        [TestCase("SELECT -257 as int16")]
        [TestCase("SELECT 80000 as uint32")]
        [TestCase("SELECT -80000 as int32")]
        [TestCase("SELECT 30000000000 as uint64")]
        [TestCase("SELECT -30000000000 as int64")]
        [Category("general")]
        public void ExecuteSelectTest(string commandText)
        {
            ExecuteTest(SYSTEM_CONNECTION_STRING, commandText, null, true);
        }

        [Test, Timeout(2400000)]
        [Category("general")]
        [Category("slow")]
        public void ExecuteLongTest()
        {
            var watch = System.Diagnostics.Stopwatch.StartNew();
            using var conn = new FireboltConnection(SYSTEM_CONNECTION_STRING);
            conn.Open();
            DbCommand command = conn.CreateCommand();
            command.CommandTimeout = 0; // make command timeout unlimited
            // Use more rows for FB2.0 since it seems to be faster
            var max = UserName == null ? 500000000000 : 250000000000;
            command.CommandText = "SELECT checksum(*) FROM generate_series(1, @max)";
            command.Prepare();
            command.Parameters.Add(CreateParameter(command, "@max", max));
            DbDataReader reader = command.ExecuteReader();
            Assert.That(reader, Is.Not.Null);
            watch.Stop();
            var elapsed = watch.ElapsedDuration().TotalSeconds;
            if (elapsed < 350)
            {
                Assert.Fail($"Expected execution time to be more than 350 sec but was {elapsed} sec");
            }
        }

        [TestCase("select * from information_schema.tables", Category = "general")]
        public void ExecuteSetEngineTest(string commandText)
        {
            ExecuteTest(USER_CONNECTION_STRING, commandText, null, false);
        }

        private static void ExecuteTest(string connString, string commandText, string? additionalCommandText, bool noNextLine)
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
            Assert.That(reader, Is.Not.Null);
            Assert.Multiple(() =>
            {
                Assert.That(reader.Read(), Is.EqualTo(true));
                Assert.That(reader.GetValue(0), Is.Not.Null);
            });
            if (noNextLine)
            {
                Assert.That(reader.Read(), Is.EqualTo(false));
            }
        }

        [Timeout(60000)]
        [TestCase(20, false)]
        [TestCase(20, true)]
        [TestCase(null, false)] // timeout is not set, i.e. default = 30 sec is used
        [TestCase(null, true)]
        [Category("general")]
        public void WithTimeout(int? timeout, bool runAsync)
        {
            using var conn = new FireboltConnection(SYSTEM_CONNECTION_STRING);
            conn.Open();
            DbCommand command = conn.CreateCommand();
            command.CommandText = LONG_QUERY;
            if (timeout != null)
            {
                command.CommandTimeout = (int)timeout;
            }

            if (runAsync)
            {
                var aggEx = Assert.ThrowsAsync<AggregateException>(command.ExecuteReaderAsync);
                Assert.That(aggEx?.InnerException, Is.InstanceOf<FireboltTimeoutException>());
            }
            else
            {
                Assert.Throws<FireboltTimeoutException>(() => command.ExecuteReader());
            }
        }

        [TestCase(true)]
        [TestCase(false)]
        public void WithZeroTimeout(bool runAsync)
        {
            using var conn = new FireboltConnection(SYSTEM_CONNECTION_STRING);
            conn.Open();
            DbCommand command = conn.CreateCommand();
            command.CommandText = "SELECT checksum(*) FROM GENERATE_SERIES(1, 3000000000)";
            command.CommandTimeout = 0;
            if (runAsync)
            {
                Assert.DoesNotThrowAsync(() => command.ExecuteReaderAsync());
            }
            else
            {
                Assert.DoesNotThrow(() => command.ExecuteReader());
            }
        }

        [Test]
        public void WithTokenCancel()
        {
            using var conn = new FireboltConnection(SYSTEM_CONNECTION_STRING);
            conn.Open();
            DbCommand command = conn.CreateCommand();
            command.CommandText = LONG_QUERY;
            using var source = new CancellationTokenSource();
            CancellationToken token = source.Token;
            var task = command.ExecuteReaderAsync(token);
            source.Cancel();
            Assert.ThrowsAsync<TaskCanceledException>(async () => await task);
        }

        [Test]
        [Category("general")]
        public void ShortTimeout()
        {
            using var conn = new FireboltConnection(SYSTEM_CONNECTION_STRING);
            conn.Open();
            FireboltCommand command = new FireboltCommand(conn, LONG_QUERY) { CommandTimeoutMillis = 1 };
            Assert.Throws<FireboltTimeoutException>(() => command.ExecuteReader());
        }

        [TestCase(0)] // 0 means infinite
        [TestCase(1)] // too short
        [TestCase(10)]
        [TestCase(10000)] // very long
        [Category("general")]
        public void CancelCommand(int timeout)
        {
            using var conn = new FireboltConnection(SYSTEM_CONNECTION_STRING);
            conn.Open();
            FireboltCancelTestCommand command = new FireboltCancelTestCommand(conn, LONG_QUERY) { CommandTimeoutMillis = timeout };
            CancellationTokenSource source = new CancellationTokenSource();
            CancellationToken cancellationToken = source.Token;
            Task<DbDataReader> task = command.ExecuteReaderAsync(cancellationToken);
            source.Cancel();
            Assert.Multiple(() =>
            {
                Assert.That(cancellationToken.IsCancellationRequested, Is.True);
                Assert.That(command.IsCancelCalled, Is.True);
            });
        }

        [TestCase("select 1", 1)]
        [TestCase("select 'hello'", "hello")]
        [TestCase("select 2, 'two'", 2)]
        [TestCase("select 'three', 3", "three")]
        [TestCase("SELECT 'one', 1 UNION ALL SELECT 'two', 2", "one")]
        [Category("general")]
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
        [Category("engine-v2")]
        public void ExecuteTestInvalidCredentialsV2()
        {
            ExecuteTestInvalidCredentials(SYSTEM_WRONG_CREDENTIALS2);
        }

        private static void ExecuteTestInvalidCredentials(string connectionString)
        {
            using var conn = new FireboltConnection(connectionString);
            FireboltException? exception = (FireboltException?)Assert.Throws(Is.InstanceOf<FireboltException>(), () => conn.Open());
            Assert.That(exception, Is.Not.Null);
            Assert.Multiple(() =>
            {
                Assert.That(exception!.Message, Does.Contain("The operation is unauthorized\nStatus: 401"));
                Assert.That(exception.ToString(), Does.Contain("FireboltDotNetSdk.Exception.FireboltException: The operation is unauthorized\nStatus: 401"));
            });
        }

        [Test]
        [Category("general")]
        public void ExecuteSelectTimestampNtz()
        {
            ExecuteDateTimeTest(
                USER_CONNECTION_STRING,
                "SELECT '2022-05-10 23:01:02.123455'::timestampntz",
                Array.Empty<string>(),
                new DateTime(2022, 5, 10, 23, 1, 2, 0).AddTicks(1234550));
        }

        [Test]
        [Category("general")]
        public void ExecuteSelectTimestampPgDate()
        {
            ExecuteDateTimeTest(
                USER_CONNECTION_STRING,
                "SELECT '2022-05-10'::pgdate",
                Array.Empty<string>(),
                new DateTime(2022, 5, 10, 0, 0, 0),
                new DateTime(2022, 5, 10, 0, 0, 0));
        }

        [Test]
        [Category("general")]
        public void ExecuteSelectTimestampTz()
        {
            using var conn = new FireboltConnection(USER_CONNECTION_STRING);
            conn.Open();
            ExecuteDateTimeTest(
                USER_CONNECTION_STRING,
                "SELECT '2022-05-10 23:01:02.123456 Europe/Berlin'::timestamptz",
                Array.Empty<string>(),
                DateTime.Parse("2022-05-10 21:01:02.123456Z"));
        }

        [Test]
        [Category("general")]
        public void ExecuteSelectTimestampTzWithMinutesInTz()
        {
            ExecuteDateTimeTest(
                USER_CONNECTION_STRING,
                "SELECT '2022-05-11 23:01:02.123123 Europe/Berlin'::timestamptz",
                new string[] { "SET time_zone=Asia/Calcutta" },
                DateTime.Parse("2022-05-11 21:01:02.123123Z"));
        }

        [Test]
        [Category("general")]
        public void ExecuteSelectTimestampTzWithTzWithMinutesAndSecondsInTz()
        {
            ExecuteDateTimeTest(
                USER_CONNECTION_STRING,
                "SELECT '1111-01-05 17:04:42.123456'::timestamptz",
                new string[] { "SET time_zone=Asia/Calcutta" },
                DateTime.Parse("1111-01-05 11:11:14.123456Z"));
        }

        [Test]
        [Category("general")]
        public void ExecuteSelectTimestampTzWithTzWithDefaultTz()
        {
            ExecuteDateTimeTest(
                USER_CONNECTION_STRING,
                "SELECT '2022-05-01 12:01:02.123456'::timestamptz",
                Array.Empty<string>(),
                DateTime.Parse("2022-05-01 12:01:02.123456Z"));
        }

        private static void ExecuteDateTimeTest(string connString, string commandText, string[] additionalCommands, DateTime expectedDateTime)
        {
            ExecuteDateTimeTest(connString, commandText, additionalCommands, expectedDateTime, expectedDateTime);
        }

        private static void ExecuteDateTimeTest(string connString, string commandText, string[] additionalCommands, DateTime expectedDateTime, object expectedValue)
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
            Assert.That(reader, Is.Not.Null);
            Assert.Multiple(() =>
            {
                Assert.That(reader.Read(), Is.EqualTo(true));
                Assert.That(reader.GetDateTime(0), Is.EqualTo(expectedDateTime));
                Assert.That(reader.GetValue(0), Is.EqualTo(expectedValue));
                Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(DateTime)));
            });
            Assert.That(reader.Read(), Is.EqualTo(false));
            conn.Close();
        }

        [TestCase("SET time_zone=Asia/Calcutta")]
        [TestCase("set time_zone=Asia/Calcutta")]
        [Category("general")]
        public void SetGoodParameterTest(string setCommand)
        {
            using var conn = new FireboltConnection(USER_CONNECTION_STRING);
            conn.Open();
            HashSet<string> expectedParamerters = new HashSet<string>() { "time_zone=Asia/Calcutta" };
            DbCommand command = conn.CreateCommand();
            command.CommandText = setCommand;
            command.ExecuteNonQuery();
            Assert.That(conn.SetParamList, Is.EqualTo(expectedParamerters));
            conn.Close();
        }

        [Test]
        [Category("general")]
        public void SetWrongParameterTest()
        {
            using var conn = new FireboltConnection(USER_CONNECTION_STRING);
            conn.Open();
            DbCommand command = conn.CreateCommand();
            command.CommandText = "SET foo=bar";
            AggregateException? outerException = (AggregateException?)Assert.Throws(Is.InstanceOf<AggregateException>(), () => command.ExecuteNonQuery());
            FireboltException? exception = (FireboltException?)outerException!.InnerExceptions[0].InnerException;
            Assert.That(exception, Is.Not.Null);
            Assert.That(exception.Response?.Trim(), Does.Contain("not allowed"));
            Assert.That(exception.Response?.Trim(), Does.Contain("foo"));
            conn.Close();
        }

        [Test]
        [Category("general")]
        public void SetGoodThenWrongParameterTest()
        {
            using var conn = new FireboltConnection(USER_CONNECTION_STRING);
            conn.Open();
            HashSet<string> expectedParamerters = new HashSet<string>() { "time_zone=Asia/Calcutta" };
            DbCommand command = conn.CreateCommand();
            command.CommandText = "SET time_zone=Asia/Calcutta";
            command.ExecuteNonQuery();
            Assert.That(conn.SetParamList, Is.EqualTo(expectedParamerters));

            command.CommandText = "SET foo=bar";
            AggregateException? outerException = (AggregateException?)Assert.Throws(Is.InstanceOf<AggregateException>(), () => command.ExecuteNonQuery());
            FireboltException? exception = (FireboltException?)outerException!.InnerExceptions[0].InnerException;
            Assert.That(exception, Is.Not.Null);
            Assert.Multiple(() =>
            {
                Assert.That(exception.Response?.Trim(), Does.Contain("not allowed"));
                Assert.That(exception.Response?.Trim(), Does.Contain("foo"));
                Assert.That(conn.SetParamList, Is.EqualTo(expectedParamerters));
            });
            conn.Close();
        }

        [Test]
        [Category("general")]
        public void ExecuteSelectBoolean()
        {
            using var conn = new FireboltConnection(USER_CONNECTION_STRING);
            conn.Open();
            DbCommand command = conn.CreateCommand();
            command.CommandText = "SELECT true, false";
            DbDataReader reader = command.ExecuteReader();
            Assert.Multiple(() =>
            {
                Assert.That(reader.Read(), Is.True);
                Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(bool)));
                Assert.That(reader.GetBoolean(0), Is.EqualTo(true));
                Assert.That(reader.GetFieldType(1), Is.EqualTo(typeof(bool)));
                Assert.That(reader.GetBoolean(1), Is.EqualTo(false));
            });
            Assert.That(reader.Read(), Is.False);
        }

        [Test]
        [Category("general")]
        public void ExecuteServiceAccountLogin()
        {
            using var conn = new FireboltConnection(SYSTEM_CONNECTION_STRING);
            conn.Open();
            DbCommand command = conn.CreateCommand();
            command.CommandText = "SELECT 1";
            DbDataReader reader = command.ExecuteReader();
            Assert.Multiple(() =>
            {
                Assert.That(reader.Read(), Is.EqualTo(true));
                Assert.That(reader.GetInt16(0), Is.EqualTo(1));
            });
            Assert.That(reader.Read(), Is.EqualTo(false));
            conn.Close();
        }

        // This test validates that reader can be used to iterate over results and retrieve string and integer values
        [Test]
        [Category("general")]
        public void ReaderSanityTest()
        {
            using var conn = new FireboltConnection(USER_CONNECTION_STRING);
            conn.Open();
            DbCommand command = conn.CreateCommand();
            command.CommandText = "SELECT table_name, length(table_name) from information_schema.tables";
            DbDataReader reader = command.ExecuteReader();
            List<string> tableNames = new List<string>();
            while (reader.Read())
            {
                string name = reader.GetString(0);
                tableNames.Add(name);
                long length = reader.GetInt64(1);
                Assert.That(length, Is.EqualTo(name.Length));
            }
        }

        [Test]
        [Category("general")]
        public void AdapterSanityTest()
        {
            DbDataAdapter adapter = new FireboltDataAdapter("SELECT table_name, length(table_name) from information_schema.tables", USER_CONNECTION_STRING);
            DataTable table = new();
            adapter.Fill(table);
            List<string> tableNames = new List<string>();
            foreach (DataRow row in table.Rows)
            {
                string? name = null;
                int? length = null;
                foreach (DataColumn column in table.Columns)
                {
                    switch (column.Ordinal)
                    {
                        case 0: name = (string)row[column]; break;
                        case 1: length = Convert.ToInt32(row[column]); break;
                    }
                }
                tableNames.Add(name!);
                Assert.That(name!, Has.Length.EqualTo(length!));
            }
        }

        [Test]
        [Category("engine-v2")]
        public void AdapterNullValuesTest()
        {
            DbDataAdapter adapter = new FireboltDataAdapter("SELECT [1,2,null]::array(int), [[1,2],[null,2],null]::array(array(int)), null::array(array(int))", USER_CONNECTION_STRING);
            var dataSet = new DataSet();

            adapter.Fill(dataSet);

            var table = dataSet.Tables[0];

            Assert.That(table.Rows, Has.Count.EqualTo(1), "Should have one row");

            var row = table.Rows[0];
            Assert.Multiple(() =>
            {

                // --- Check column types ---
                Assert.That(table.Columns[0].DataType, Is.EqualTo(typeof(int?[])));
                Assert.That(table.Columns[1].DataType, Is.EqualTo(typeof(int?[][])));
                Assert.That(table.Columns[2].DataType, Is.EqualTo(typeof(int[][])));
            });

            var col0 = row.Field<int?[]>(0)!;
            var col1 = row.Field<int?[][]>(1)!;
            var isCol2Null = row.IsNull(2);

            CollectionAssert.AreEqual(new int?[] { 1, 2, null }, col0.ToArray(), "Column 0 mismatch");

            Assert.That(col1, Has.Length.EqualTo(3), "col1 outer array length mismatch");
            CollectionAssert.AreEqual(new int?[] { 1, 2 }, col1[0], "col1[0] mismatch");
            CollectionAssert.AreEqual(new int?[] { null, 2 }, col1[1], "col1[1] mismatch");
            Assert.Multiple(() =>
            {
                Assert.That(col1[2], Is.Null, "col1[2] should be null");
                Assert.That(isCol2Null, Is.True, "Column 2 should be NULL");
            });
        }

        [Test]
        [Category("engine-v2")]
        public void AdapterVariousValuesTest()
        {
            const string query = "select  1                                                         as col_int,\n" +
                                 "        null::int                                                 as col_int_null,\n" +
                                 "        30000000000                                               as col_long,\n" +
                                 "        null::bigint                                              as col_long_null,\n" +
                                 "        1.23::float4                                              as col_float,\n" +
                                 "        null::float4                                              as col_float_null,\n" +
                                 "        1.23456789012                                             as col_double,\n" +
                                 "        null::double                                              as col_double_null,\n" +
                                 "        'text'                                                    as col_text,\n" +
                                 "        null::text                                                as col_text_null,\n" +
                                 "        '2021-03-28'::date                                        as col_date,\n" +
                                 "        null::date                                                as col_date_null,\n" +
                                 "        '2019-07-31 01:01:01'::timestamp                          as col_timestamp,\n" +
                                 "        null::timestamp                                           as col_timestamp_null,\n" +
                                 "        '1111-01-05 17:04:42.123456'::timestamptz                 as col_timestamptz,\n" +
                                 "        null::timestamptz                                         as col_timestamptz_null,\n" +
                                 "        true                                                      as col_boolean,\n" +
                                 "        null::bool                                                as col_boolean_null,\n" +
                                 "        [1,2,3,4]                                                 as col_array,\n" +
                                 "        null::array(int)                                          as col_array_null,\n" +
                                 "        [1,2,null]::array(int)                                    as col_array_null_int,\n" +
                                 "        '1231232.123459999990457054844258706536'::decimal(38, 30) as col_decimal,\n" +
                                 "        null::decimal(38, 30)                                     as col_decimal_null,\n" +
                                 "        'abc123'::bytea                                           as col_bytea,\n" +
                                 "        null::bytea                                               as col_bytea_null,\n" +
                                 "        'point(1 2)'::geography                                   as col_geography,\n" +
                                 "        null::geography                                           as col_geography_null,\n" +
                                 "        [[1,2],[null,2],null]::array(array(int))                  as col_arr_arr,\n" +
                                 "        null::array(array(int))                                   as col_arr_arr_null";
            DbDataAdapter adapter = new FireboltDataAdapter(query, USER_CONNECTION_STRING);
            var dataSet = new DataSet();

            adapter.Fill(dataSet);
            var types = new List<Type>
            {
                typeof(int), typeof(int), typeof(long), typeof(long),
                typeof(float), typeof(float), typeof(double), typeof(double),
                typeof(string), typeof(string), typeof(DateTime), typeof(DateTime),
                typeof(DateTime), typeof(DateTime), typeof(DateTime), typeof(DateTime),
                typeof(bool), typeof(bool), typeof(int[]), typeof(int[]), typeof(int?[]),
                typeof(decimal), typeof(decimal), typeof(byte[]), typeof(byte[]),
                typeof(string), typeof(string), typeof(int?[][]), typeof(int[][])
            };

            var table = dataSet.Tables[0];

            for (var i = 0; i < types.Count; i++)
            {
                Assert.That(table.Columns[i].DataType, Is.EqualTo(types[i]));
            }
        }

        [Test]
        [Category("v1")]
        public void ExecuteServiceAccountLoginWithInvalidCredentialsV1()
        {
            ExecuteServiceAccountLoginWithInvalidCredentials(SYSTEM_WRONG_CREDENTIALS1);
        }

        [Test]
        [Category("v2")]
        [Category("engine-v2")]
        public void ExecuteServiceAccountLoginWithInvalidCredentialsV2()
        {
            ExecuteServiceAccountLoginWithInvalidCredentials(SYSTEM_WRONG_CREDENTIALS2);
        }

        private static void ExecuteServiceAccountLoginWithInvalidCredentials(string connectionString)
        {
            using var conn = new FireboltConnection(connectionString);
            FireboltException? exception = (FireboltException?)Assert.Throws(Is.InstanceOf<FireboltException>(), () => conn.Open());
            Assert.That(exception, Is.Not.Null);
            Assert.That(exception!.Message, Does.Contain("401"), $"Expected Unauthorized (401) error message but was {exception!.Message}");
        }

        [TestCase(nameof(Password), Category = "v1")]
        [TestCase(nameof(ClientSecret), Category = "v2,engine-v2")]
        public void ExecuteServiceAccountLoginWithMissingSecret(string secretField)
        {
            var connString = ConnectionString(new Tuple<string, string?>(nameof(Engine), null), new Tuple<string, string?>(secretField, ""));
            FireboltException? exception = (FireboltException?)Assert.Throws(Is.InstanceOf<FireboltException>(), () => new FireboltConnection(connString));
            Assert.That(exception, Is.Not.Null);
            Assert.That(exception!.Message, Is.EqualTo("Configuration error: either Password or ClientSecret must be provided but not both"));
        }

        [Test]
        [Category("general")]
        public void ExecuteSelectArrayNullable()
        {
            ExecuteTest(SYSTEM_CONNECTION_STRING, "select [1,NULL,3]", new int?[] { 1, null, 3 }, typeof(int?[]));
        }

        [Test]
        [Category("general")]
        public void ExecuteSelectTwoDimensionalArrayNullable()
        {
            ExecuteTest(SYSTEM_CONNECTION_STRING, "select [[1,NULL,3]]", new int?[1][] { new int?[] { 1, null, 3 } }, typeof(int?[][]));
        }

        [Test]
        [Category("general")]
        public void ExecuteSelectArray()
        {
            ExecuteTest(SYSTEM_CONNECTION_STRING, "select [1,3]", new int?[] { 1, 3 }, typeof(int[]));
        }

        [Test]
        [Category("general")]
        public void ExecuteSelectTwoDimensionalArray()
        {
            ExecuteTest(SYSTEM_CONNECTION_STRING, "select [[1,3]]", new int?[1][] { new int?[] { 1, 3 } }, typeof(int[][]));
        }

        [TestCase("select array_agg(arr) from (select array_agg(n) arr from (SELECT n FROM GENERATE_SERIES(1, 10, 1) s(n) ) group by n / 2)", typeof(int[][]))]
        [TestCase("select array_agg(n) arr from (SELECT n FROM GENERATE_SERIES(1, 10, 1) s(n) ) group by n / 2", typeof(int[]))]
        [TestCase("SELECT n FROM GENERATE_SERIES(1, 10, 1) s(n)", typeof(int))]
        [Category("engine-v2")]
        public void ExecuteDifferentArrays(string commandText, Type expectedType)
        {
            DbConnection connection = new FireboltConnection(USER_CONNECTION_STRING);
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = commandText;
            var reader = command.ExecuteReader();
            var type = reader.GetFieldType(0);
            reader.Read();
            var value = reader.GetValue(0);
            Assert.That(value.GetType(), Is.EqualTo(type));
            Assert.That(value.GetType(), Is.EqualTo(expectedType));
        }

        private static void ExecuteTest(string connString, string commandText, object expectedValue, Type expectedType)
        {
            var conn = new FireboltConnection(connString);
            conn.Open();
            DbDataReader reader = ExecuteTest(conn, commandText, expectedValue, expectedType);
            Assert.That(reader.Read(), Is.EqualTo(false));
            conn.Close();
        }

        private static DbDataReader ExecuteTest(DbConnection conn, string commandText, object expectedValue, Type? expectedType)
        {
            DbCommand command = conn.CreateCommand();
            command.CommandText = commandText;
            DbDataReader reader = command.ExecuteReader();
            Assert.That(reader, Is.Not.Null);
            Assert.Multiple(() =>
            {
                Assert.That(reader.Read(), Is.EqualTo(true));
                Assert.That(reader.GetValue(0), Is.EqualTo(expectedValue));
            });
            if (expectedType != null)
            {
                Assert.That(reader.GetFieldType(0), Is.EqualTo(expectedType));
            }
            return reader;
        }

        [Test]
        [Category("general")]
        public void ExecuteSelectByteA()
        {
            ExecuteTest(SYSTEM_CONNECTION_STRING, "SELECT 'hello_world_123ツ\n\u0048'::bytea", Encoding.UTF8.GetBytes("hello_world_123ツ\n\u0048"), typeof(byte[]));
        }

        [Test]
        [Category("general")]
        public void ShouldThrowExceptionWhenEngineIsNotFound()
        {
            var connString = $"{SYSTEM_CONNECTION_STRING};engine=InexistantEngine";
            using var conn = new FireboltConnection(connString);
            FireboltException? exception = (FireboltException?)Assert.Throws(Is.InstanceOf<FireboltException>(), () => conn.Open());
            Assert.That(exception!.Message,
                Does.Match(
                    "Engine.+InexistantEngine.+not"
                ).Or.Match(
                    // V2 engines return 404, for which we don't report the reponse body but only the status code
                    // This will be fixed in FIR-32894
                    ".+unexpected status code.+"
                )
            );
        }

        [Test]
        [Category("general")]
        public void Select1()
        {
            ExecuteTest(USER_CONNECTION_STRING, "SELECT 1", 1, typeof(int));
        }

        [Test]
        [Category("general")]
        public void WrongQuerySystemEngine()
        {
            WrongQuery(SYSTEM_CONNECTION_STRING);
        }

        [Test]
        [Category("general")]
        public void WrongQueryUserEngine()
        {
            WrongQuery(USER_CONNECTION_STRING);
        }

        private static void WrongQuery(string connString)
        {
            using (var conn = new FireboltConnection(connString))
            {
                conn.Open();
                DbCommand command = conn.CreateCommand();
                command.CommandText = "wrong query";
                Assert.Throws(Is.InstanceOf<FireboltException>(), () => command.ExecuteReader());
                Assert.Throws(Is.InstanceOf<FireboltException>(), () => command.ExecuteNonQuery());
            }
        }

        [TestCase("SELECT 'inf'::float", double.PositiveInfinity, float.PositiveInfinity, typeof(float), Category = "v1,v2")]
        [TestCase("SELECT '-inf'::float", double.NegativeInfinity, float.NegativeInfinity, typeof(float), Category = "v1,v2")]
        [TestCase("SELECT 'inf'::float", double.PositiveInfinity, float.PositiveInfinity, typeof(double), Category = "engine-v2")]
        [TestCase("SELECT '-inf'::float", double.NegativeInfinity, float.NegativeInfinity, typeof(double), Category = "engine-v2")]
        public void SelectInfinity(string query, double doubleExpected, float floatExpected, Type expectedType)
        {
            var conn = new FireboltConnection(USER_CONNECTION_STRING);
            conn.Open();
            DbDataReader reader = ExecuteTest(conn, query, doubleExpected, expectedType);
            Assert.Multiple(() =>
            {
                Assert.That(reader.GetDouble(0), Is.EqualTo(doubleExpected));
                Assert.That(reader.GetFloat(0), Is.EqualTo(floatExpected));
                Assert.Throws<InvalidCastException>(() => reader.GetInt16(0));
                Assert.Throws<InvalidCastException>(() => reader.GetInt32(0));
                Assert.Throws<InvalidCastException>(() => reader.GetInt64(0));
            });
            Assert.That(reader.Read(), Is.EqualTo(false));
            conn.Close();
        }

        [TestCase("SELECT 'NaN'::float", typeof(float), Category = "v1,v2")]
        [TestCase("SELECT '-NaN'::float", typeof(float), Category = "v1,v2")]
        [TestCase("SELECT 'NaN'::float", typeof(double), Category = "engine-v2")]
        [TestCase("SELECT '-NaN'::float", typeof(double), Category = "engine-v2")]
        public void SelectNaN(string query, Type expectedType)
        {
            var conn = new FireboltConnection(USER_CONNECTION_STRING); conn.Open();
            DbDataReader reader = ExecuteTest(conn, query, double.NaN, expectedType);
            Assert.Multiple(() =>
            {
                Assert.That(double.IsNaN(reader.GetDouble(0)), Is.True);
                Assert.That(float.IsNaN(reader.GetFloat(0)), Is.True);
            });
            Assert.Multiple(() =>
            {
                Assert.Throws<InvalidCastException>(() => reader.GetInt16(0));
                Assert.Throws<InvalidCastException>(() => reader.GetInt32(0));
                Assert.Throws<InvalidCastException>(() => reader.GetInt64(0));
            });
            Assert.That(reader.Read(), Is.EqualTo(false));
            conn.Close();
        }

        [Test]
        [Category("general")]
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
                        Assert.Multiple(() =>
                        {
                            Assert.That(reader.GetInt32(0), Is.EqualTo(1));
                            Assert.That(reader.GetDecimal(1), Is.EqualTo(2));
                            Assert.That(reader.GetInt64(2), Is.EqualTo(1234567890L));
                            Assert.That(Math.Abs(reader.GetFloat(3) - 3.14), Is.LessThanOrEqualTo(0.001));
                            Assert.That(Math.Abs(reader.GetDecimal(4) - 6.02M), Is.LessThanOrEqualTo(0.001M));
                            Assert.That(Math.Abs(reader.GetDouble(5) - 2.718281828), Is.LessThanOrEqualTo(0.001));
                            Assert.That(reader.GetString(6), Is.EqualTo("hello"));
                            Assert.That(reader.GetDateTime(7), Is.EqualTo(TypesConverter.ParseDateTime("2023-10-18")));
                            Assert.That(reader.GetDateTime(8), Is.EqualTo(TypesConverter.ParseDateTime("2023-10-18 17:30:20.123456")));
                            Assert.That(reader.GetDateTime(9), Is.EqualTo(TypesConverter.ParseDateTime("2023-10-18 17:30:20.123456+02")));
                            Assert.That(reader.GetBoolean(10), Is.EqualTo(true));
                            // byte array
                            Assert.That(reader.GetValue(11), Is.EqualTo(bytes));
                        });
                        byte[] buffer = new byte[bytes.Length];
                        long length = reader.GetBytes(11, 0, buffer, 0, buffer.Length);
                        Assert.Multiple(() =>
                        {
                            Assert.That(length, Is.EqualTo(buffer.Length));
                            Assert.That(buffer, Is.EqualTo(bytes));
                        });
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

                        Assert.Multiple(() =>
                        {
                            Assert.That(record.GetInt32(0), Is.EqualTo(1));
                            Assert.That(record.GetDecimal(1), Is.EqualTo(2));
                            Assert.That(record.GetInt64(2), Is.EqualTo(1234567890L));
                            Assert.That(Math.Abs(record.GetFloat(3) - 3.14), Is.LessThanOrEqualTo(0.001));
                            Assert.That(Math.Abs(record.GetDecimal(4) - 6.02M), Is.LessThanOrEqualTo(0.001M));
                            Assert.That(Math.Abs(record.GetDouble(5) - 2.718281828), Is.LessThanOrEqualTo(0.001));
                            Assert.That(record.GetString(6), Is.EqualTo("hello"));
                            Assert.That(record.GetDateTime(7), Is.EqualTo(TypesConverter.ParseDateTime("2023-10-18")));
                            Assert.That(record.GetDateTime(8), Is.EqualTo(TypesConverter.ParseDateTime("2023-10-18 17:30:20.123456")));
                            Assert.That(record.GetDateTime(9), Is.EqualTo(TypesConverter.ParseDateTime("2023-10-18 17:30:20.123456+02")));
                            Assert.That(record.GetBoolean(10), Is.EqualTo(true));
                            // byte array
                            Assert.That(record.GetValue(11), Is.EqualTo(bytes));
                        });
                        byte[] buffer = new byte[bytes.Length];
                        long length = record.GetBytes(11, 0, buffer, 0, buffer.Length);
                        Assert.Multiple(() =>
                        {
                            Assert.That(length, Is.EqualTo(buffer.Length));
                            Assert.That(buffer, Is.EqualTo(bytes));
                        });
                    }
                }

                Assert.Multiple(() =>
                {
                    Assert.That(selectOneRow(conn, 1), Is.True);
                    Assert.That(selectOneRow(conn, 0), Is.False);
                });
                CreateCommand(conn, DROP_SIMPLE_TABLE).ExecuteNonQuery();
            }
        }

        [TestCase("It's a test", "It's a test", false)] // Single quote escaping
        [TestCase("Path\\to\\file", "Path\\to\\file", false)] // Backslash escaping
        [TestCase("Text\0with\0nulls", "Text\0with\0nulls", false)] // Null character escaping
        [TestCase("Line1\nLine2\nLine3", "Line1\nLine2\nLine3", false)] // Newline character
        [TestCase("Line1\rLine2", "Line1\rLine2", false)] // Carriage return character
        [TestCase("Column1\tColumn2\tColumn3", "Column1\tColumn2\tColumn3", false)] // Tab character
        [TestCase("It's a 'complex' string\\with\nnewlines\tand\ttabs", "It's a 'complex' string\\with\nnewlines\tand\ttabs", false)] // Mixed special characters
        [TestCase("", "", false)] // Empty string
        [TestCase(null, null, true)] // Null string
        [Category("general")]
        public void StringParameterEscapingTest(string? inputValue, string? expectedValue, bool expectNull)
        {
            using (var conn = new FireboltConnection(USER_CONNECTION_STRING))
            {
                conn.Open();

                DbCommand command = CreateCommand(conn, "SELECT @text");
                command.Parameters.Add(CreateParameter(command, "@text", inputValue));
                
                using (DbDataReader reader = command.ExecuteReader())
                {
                    Assert.That(reader.Read(), Is.True);
                    
                    if (expectNull)
                    {
                        Assert.That(reader.IsDBNull(0), Is.True);
                    }
                    else
                    {
                        Assert.That(reader.GetString(0), Is.EqualTo(expectedValue));
                    }
                    
                    Assert.That(reader.Read(), Is.False);
                }
            }
        }

        [Test]
        [Category("general")]
        public void CreateDropFillTableWithEmptyArrays()
        {
            CreateDropFillTableWithArrays("int", Array.Empty<int>(), typeof(int?[]), "int", Array.Empty<int[]>(), typeof(int?[][]), "int", Array.Empty<int[][]>(), typeof(int?[][][]));
        }

        [Test]
        [Category("general")]
        public void CreateDropFillTableWithNullArrays()
        {
            CreateDropFillTableWithArrays("int", null, typeof(int?[]), "int", null, typeof(int?[][]), "int", null, typeof(int?[][][]));
        }

        [Test]
        [Category("general")]
        public void CreateDropFillTableWithSingetonArrays()
        {
            CreateDropFillTableWithArrays(
                "int", new int[] { 1 }, typeof(int?[]),
                "int", new int[][] { new int[] { 22 } }, typeof(int?[][]),
                "int", new int[][][] { new int[][] { new int[] { 333 } } }, typeof(int?[][][]));
        }

        [Test]
        [Category("general")]
        public void CreateDropFillTableWithFullArrays()
        {
            CreateDropFillTableWithArrays(
                "int", new int[] { 1, 2, 3 }, typeof(int?[]),
                "int", new int[][] { new int[] { 21, 22, 23 }, new int[] { 31, 32, 33 } }, typeof(int?[][]),
                "int", new int[][][] { new int[][] { new int[] { 333, 334, 335, 336 }, new int[] { 1, 2 } } }, typeof(int?[][][])
            );
        }

        [Test]
        [Category("v1")]
        public void CreateDropFillTableWithArraysOfDifferentTypes()
        {
            CreateDropFillTableWithArrays(
                "long", new long[] { 1, 2 }, typeof(long?[]),
                "float", new float[][] { new float[] { 2.7F, 3.14F } }, typeof(float?[][]),
                "double", new double[][][] { new double[][] { new double[] { 3.1415926 } } }, typeof(double?[][][]));
        }

        [Test]
        [Category("v2")]
        [Category("engine-v2")]
        public void CreateDropFillTableWithArraysOfDifferentTypesV2()
        {
            CreateDropFillTableWithArrays(
                "long", new long[] { 1, 2 }, typeof(long?[]),
                "float", new double[][] { new double[] { 2.7, 3.14 } }, typeof(double?[][]), // float is alias to double in engine V2
                "double", new double[][][] { new double[][] { new double[] { 3.1415926 } } }, typeof(double?[][][]));
        }

        [Test]
        [Category("v2")]
        [Category("engine-v2")]
        public void ThrowsStructuredExceptionOnJSONErrorBody()
        {
            using var conn = new FireboltConnection(SYSTEM_CONNECTION_STRING);
            conn.Open();
            CreateCommand(conn, "SET advanced_mode=1").ExecuteNonQuery();
            CreateCommand(conn, "SET enable_json_error_output_format=true").ExecuteNonQuery();
            DbCommand command = conn.CreateCommand();
            command.CommandText = "SELECT 'blue'::int";
            FireboltException exception = Assert.Throws<FireboltStructuredException>(() => command.ExecuteReader());
            Assert.That(exception.Message, Does.Contain("Unable to cast text 'blue' to integer"));
        }

        [Test]
        [Category("v2")]
        [Category("engine-v2")]
        public void SelectInsertGeography()
        {
            var conn = new FireboltConnection(USER_CONNECTION_STRING); conn.Open();
            String cleanup = "DROP TABLE IF EXISTS test_geography";
            String createTableQuery = "CREATE FACT TABLE test_geography (g GEOGRAPHY)";
            String insertQuery = "INSERT INTO test_geography (g) VALUES (@g)";
            String selectQuery = "SELECT g FROM test_geography";
            String point = "POINT(1 2)";
            String pointHex = "0101000020E6100000FEFFFFFFFFFFEF3F0000000000000040";

            CreateCommand(conn, cleanup).ExecuteNonQuery();
            CreateCommand(conn, createTableQuery).ExecuteNonQuery();
            DbCommand insert = CreateCommand(conn, insertQuery);
            insert.Prepare();
            insert.Parameters.Add(CreateParameter(insert, "@g", point));
            insert.ExecuteNonQuery();

            DbCommand select = CreateCommand(conn, selectQuery);
            using (DbDataReader reader = select.ExecuteReader())
            {
                Assert.Multiple(() =>
                {
                    Assert.That(reader.Read(), Is.EqualTo(true));
                    Assert.That(reader.GetString(0), Is.EqualTo(pointHex));
                    Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(string)));
                });
                Assert.That(reader.Read(), Is.EqualTo(false));
            }
        }

        [Test]
        [Category("v2")]
        [Category("engine-v2")]
        public void TestSelectStruct()
        {
            string[] setupSQL = {
                "SET advanced_mode=1",
                "SET enable_create_table_v2=true",
                "SET enable_struct_syntax=true",
                "SET prevent_create_on_information_schema=true",
                "SET enable_create_table_with_struct_type=true",
                "DROP TABLE IF EXISTS test_struct",
                "DROP TABLE IF EXISTS test_struct_helper",
                "CREATE TABLE IF NOT EXISTS test_struct(id int not null, s struct(a array(int) null, b datetime null) not null)",
                "CREATE TABLE IF NOT EXISTS test_struct_helper(a array(int) null, b datetime null)",
                "INSERT INTO test_struct_helper(a, b) VALUES ([1, 2], '2019-07-31 01:01:01')",
                "INSERT INTO test_struct(id, s) SELECT 1, test_struct_helper FROM test_struct_helper"
            };
            var conn = new FireboltConnection(USER_CONNECTION_STRING); conn.Open();
            for (int i = 0; i < setupSQL.Length; i++)
            {
                CreateCommand(conn, setupSQL[i]).ExecuteNonQuery();
            }

            DbCommand select = CreateCommand(conn, "SELECT test_struct FROM test_struct");
            using (DbDataReader reader = select.ExecuteReader())
            {
                Assert.That(reader.Read(), Is.EqualTo(true));
                var type = reader.GetFieldType(0);
                Assert.That(type, Is.EqualTo(typeof(Dictionary<string, object>)));

                Dictionary<string, object> s = (Dictionary<string, object>)reader.GetValue(0);
                Assert.Multiple(() =>
                {
                    Assert.That(s["id"], Is.EqualTo(1));
                    Assert.That(s["s"], Is.InstanceOf<Dictionary<string, object>>());
                });
                Dictionary<string, object> s2 = (Dictionary<string, object>)s["s"];
                Assert.Multiple(() =>
                {
                    Assert.That(s2["a"], Is.EqualTo(new[] { 1, 2 }));
                    Assert.That(s2["b"], Is.EqualTo(TypesConverter.ParseDateTime("2019-07-31 01:01:01")));
                });
            }
        }

        [Test]
        [Category("v2")]
        [Category("engine-v2")]
        public void TestSelectLargeDecimal()
        {
            string sql = "SELECT 12345678901234567890123456789.123456789::decimal(38, 9)";
            using (var conn = new FireboltConnection(USER_CONNECTION_STRING))
            {
                conn.Open();
                DbCommand command = conn.CreateCommand();
                command.CommandText = sql;
                DbDataReader reader = command.ExecuteReader();
                Assert.Multiple(() =>
                {
                    Assert.That(reader.Read(), Is.EqualTo(true));
                    Assert.That(reader.GetDecimal(0), Is.EqualTo(12345678901234567890123456789.123456789M));
                });
                Assert.That(reader.Read(), Is.EqualTo(false));
            }
        }

        private static void CreateDropFillTableWithArrays(string type1, Array? inta1, Type expType1, string type2, Array? inta2, Type expType2, string type3, Array? inta3, Type expType3)
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
                            Assert.Multiple(() =>
                            {
                                Assert.That(reader.GetInt32(0), Is.EqualTo(id));
                                Assert.That(reader.GetValue(1), Is.EqualTo(inta1 == null ? DBNull.Value : inta1));
                                Assert.That(reader.GetValue(2), Is.EqualTo(inta2 == null ? DBNull.Value : inta2));
                                Assert.That(reader.GetValue(3), Is.EqualTo(inta3 == null ? DBNull.Value : inta3));
                            });
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

                            Assert.Multiple(() =>
                            {
                                Assert.That(record.GetInt32(0), Is.EqualTo(id));
                                Assert.That(record.GetValue(1), Is.EqualTo(inta1 == null ? DBNull.Value : inta1));
                                Assert.That(record.GetValue(2), Is.EqualTo(inta2 == null ? DBNull.Value : inta2));
                                Assert.That(record.GetValue(3), Is.EqualTo(inta3 == null ? DBNull.Value : inta3));
                            });
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
        private static bool selectOneRow(DbConnection conn, int id)
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

        private static DbCommand CreateCommand(DbConnection conn, string query)
        {
            DbCommand command = conn.CreateCommand();
            command.CommandText = query;
            return command;
        }

        class FireboltCancelTestCommand : FireboltCommand
        {
            internal bool IsCancelCalled { get; private set; }
            public FireboltCancelTestCommand(FireboltConnection? connection, string? commandText, params DbParameter[] parameters) : base(connection, commandText, parameters)
            {
                IsCancelCalled = false;
            }

            public override void Cancel()
            {
                IsCancelCalled = true;
            }
        }
    }
}
