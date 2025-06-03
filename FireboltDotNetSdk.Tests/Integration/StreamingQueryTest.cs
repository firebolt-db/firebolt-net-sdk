using FireboltDotNetSdk.Client;
using FireboltNETSDK.Exception;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    internal class StreamingQueryTest : IntegrationTest
    {
        private static readonly string UserConnectionString = ConnectionString();
        private const string VariousTypeQuery =
                "select  1                                                         as col_int,\n" +
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

        private static readonly List<Type> TypeList = new()
        {
            typeof(int), typeof(int?), typeof(long), typeof(long?),
            typeof(float), typeof(float?), typeof(double), typeof(double?),
            typeof(string), typeof(string), typeof(DateTime), typeof(DateTime?),
            typeof(DateTime), typeof(DateTime?), typeof(DateTime), typeof(DateTime?),
            typeof(bool), typeof(bool?), typeof(int[]), typeof(int[]), typeof(int?[]),
            typeof(decimal), typeof(decimal?), typeof(byte[]), typeof(byte?[]),
            typeof(string), typeof(string), typeof(int?[][]), typeof(int[][])
        };

        [Test]
        [Category("engine-v2")]
        [Category("slow")]
        public async Task ExecuteStreamedQueryAsyncAndValidateSum()
        {
            await using var connection = new FireboltConnection(UserConnectionString);
            await connection.OpenAsync();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "select 1 from generate_series(1, 2500000)";

            await using var reader = await command.ExecuteStreamedQueryAsync();
            var sum = 0L;
            while (await reader.ReadAsync())
            {
                sum += reader.GetInt64(0);
            }

            Assert.That(sum, Is.EqualTo(2500000));
        }

        [Test]
        [Category("engine-v2")]
        public async Task ExecuteStreamedQueryAsyncWithSyntaxError()
        {
            await using var connection = new FireboltConnection(UserConnectionString);
            await connection.OpenAsync();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "select *1;";

            var exception = Assert.ThrowsAsync<FireboltStructuredException>(() => command.ExecuteStreamedQueryAsync());
            Assert.That(exception, Is.Not.Null);
            Assert.That(exception!.Message, Does.Contain("Line 1, Column 9: syntax error, unexpected integer, expecting end of file"));
            Assert.That(exception!.Message, Does.Contain("select *1;"));
        }

        [Test]
        [Category("engine-v2")]
        public async Task ExecuteStreamedQueryAsyncWithDivisionByZeroError()
        {
            await using var connection = new FireboltConnection(UserConnectionString);
            await connection.OpenAsync();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "select 1/(i-100000) as a from generate_series(1,100000) as i";
            var oneReadAtLeast = false;
            var exceptionThrown = false;

            await using var reader = await command.ExecuteStreamedQueryAsync();
            try
            {
                while (await reader.ReadAsync())
                {
                    oneReadAtLeast = true;
                }
            }
            catch (System.Exception e)
            {
                exceptionThrown = true;
                Assert.That(e, Is.Not.Null);
                Assert.That(e, Is.TypeOf(typeof(FireboltStructuredException)));
                Assert.That(e.Message, Does.Contain("Line 1, Column 9: Division by zero\n" +
                                                             "select 1/(i-100000) as a from generate_series(1,...\n"));
            }
            Assert.Multiple(() =>
            {
                Assert.That(oneReadAtLeast, Is.True, "Expected at least one row to be read before the exception was thrown.");
                Assert.That(exceptionThrown, Is.True, "Expected an exception to be thrown.");
            });
        }

        [Test]
        [Category("engine-v2")]
        public async Task ExecuteStreamedQueryAsyncWithDifferentDataTypes()
        {
            await using var connection = new FireboltConnection(UserConnectionString);
            await connection.OpenAsync();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = VariousTypeQuery;

            await using var reader = await command.ExecuteStreamedQueryAsync();
            Assert.That(await reader.ReadAsync(), Is.EqualTo(true));
            for (var i = 0; i < TypeList.Count; i++)
            {
                Assert.That(reader.GetFieldType(i), Is.EqualTo(TypeList[i]));
            }
        }

        [Test]
        [Category("engine-v2")]
        public async Task ExecuteStreamedQueryAsyncWithPreparedStatement()
        {
            await using var connection = new FireboltConnection(UserConnectionString);
            await connection.OpenAsync();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "select 1 from generate_series(1, @max)";
            command.Parameters.Add(CreateParameter(command, "@max", 2));

            await using var reader = await command.ExecuteStreamedQueryAsync();
            Assert.Multiple(() =>
            {
                Assert.That(reader.Read(), Is.EqualTo(true));
                Assert.That(reader.GetInt32(0), Is.EqualTo(1));
                Assert.That(reader.Read(), Is.EqualTo(true));
                Assert.That(reader.GetInt32(0), Is.EqualTo(1));
                Assert.That(reader.Read(), Is.EqualTo(false), "Expected no more rows to be read after the second row.");
            });
        }

        [Test]
        [Category("engine-v2")]
        [Category("slow")]
        public void ExecuteStreamedQueryAndValidateSum()
        {
            using var connection = new FireboltConnection(UserConnectionString);
            connection.Open();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "select 1 from generate_series(1, 2500000)";

            using var reader = command.ExecuteStreamedQuery();
            var sum = 0L;
            while (reader.Read())
            {
                sum += reader.GetInt64(0);
            }

            Assert.That(sum, Is.EqualTo(2500000));
        }

        [Test]
        [Category("engine-v2")]
        public void ExecuteStreamedQueryWithSyntaxError()
        {
            using var connection = new FireboltConnection(UserConnectionString);
            connection.Open();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "select *1;";

            var exception = Assert.Throws<FireboltStructuredException>(() => command.ExecuteStreamedQuery());
            Assert.That(exception, Is.Not.Null);
            Assert.That(exception!.Message, Does.Contain("Line 1, Column 9: syntax error, unexpected integer, expecting end of file"));
            Assert.That(exception!.Message, Does.Contain("select *1;"));
        }

        [Test]
        [Category("engine-v2")]
        public void ExecuteStreamedQueryWithDivisionByZeroError()
        {
            using var connection = new FireboltConnection(UserConnectionString);
            connection.Open();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "select 1/(i-100000) as a from generate_series(1,100000) as i";
            var oneReadAtLeast = false;
            var exceptionThrown = false;

            using var reader = command.ExecuteStreamedQuery();
            try
            {
                while (reader.Read())
                {
                    oneReadAtLeast = true;
                }
            }
            catch (System.Exception e)
            {
                exceptionThrown = true;
                Assert.That(e, Is.Not.Null);
                Assert.That(e, Is.TypeOf(typeof(FireboltStructuredException)));
                Assert.That(e.Message, Does.Contain("Line 1, Column 9: Division by zero\n" +
                                                             "select 1/(i-100000) as a from generate_series(1,...\n"));
            }
            Assert.Multiple(() =>
            {
                Assert.That(oneReadAtLeast, Is.True, "Expected at least one row to be read before the exception was thrown.");
                Assert.That(exceptionThrown, Is.True, "Expected an exception to be thrown.");
            });
        }

        [Test]
        [Category("engine-v2")]
        public void ExecuteStreamedQueryWithDifferentDataTypes()
        {
            using var connection = new FireboltConnection(UserConnectionString);
            connection.Open();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = VariousTypeQuery;

            using var reader = command.ExecuteStreamedQuery();
            Assert.That(reader.Read(), Is.EqualTo(true));
            for (var i = 0; i < TypeList.Count; i++)
            {
                Assert.That(reader.GetFieldType(i), Is.EqualTo(TypeList[i]));
            }
        }

        [Test]
        [Category("engine-v2")]
        public void ExecuteStreamedQueryWithPreparedStatement()
        {
            using var connection = new FireboltConnection(UserConnectionString);
            connection.Open();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "select 1 from generate_series(1, @max)";
            command.Parameters.Add(CreateParameter(command, "@max", 2));

            using var reader = command.ExecuteStreamedQuery();
            Assert.Multiple(() =>
            {
                Assert.That(reader.Read(), Is.EqualTo(true));
                Assert.That(reader.GetInt32(0), Is.EqualTo(1));
                Assert.That(reader.Read(), Is.EqualTo(true));
                Assert.That(reader.GetInt32(0), Is.EqualTo(1));
                Assert.That(reader.Read(), Is.EqualTo(false), "Expected no more rows to be read after the second row.");
            });
        }
    }
}
