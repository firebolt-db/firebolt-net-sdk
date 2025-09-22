using System.Data;
using System.Data.Common;
using System.Text;
using FireboltDotNetSdk.Client;
using static System.DateTime;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    internal class DataTypesTest : IntegrationTest
    {
        private static readonly string UserConnectionString = ConnectionString();
        // Legacy combined test retained for compatibility; specific matrix tests are parameterized below
        private const string VariousTypeQuery =
                // int
                "select  1                                     as col_int,\n" +
                "        null::int                             as col_int_null,\n" +
                "        [1,2]                                 as col_int_array,\n" +
                "        [1,null]                              as col_int_null_array,\n" +
                "        null::array(int)                      as col_int_array_null,\n" +
                "        [[1,2],null]::array(array(int))       as col_int_array_null_array,\n" +
                "        [[1,null],null]::array(array(int))    as col_int_null_array_null_array,\n" +
                "        null::array(array(int))               as col_int_array_array_null,\n" +
                // long
                "        30000000000                           as col_long,\n" +
                "        null::bigint                          as col_long_null,\n" +
                "        [1,2,3]::array(bigint)                as col_long_array,\n" +
                "        [1,null]::array(bigint)               as col_long_null_array,\n" +
                "        null::array(bigint)                   as col_long_array_null,\n" +
                "        [[1,2],null]::array(array(bigint))    as col_long_array_null_array,\n" +
                "        [[1,null],null]::array(array(bigint)) as col_long_null_array_null_array,\n" +
                "        null::array(array(bigint))            as col_long_array_array_null,\n" +
                // float
                "        1.23::float4                             as col_float,\n" +
                "        null::float4                             as col_float_null,\n" +
                "        [1.25,2.5]::array(float4)                as col_float_array,\n" +
                "        [1.25,null]::array(float4)               as col_float_null_array,\n" +
                "        null::array(float4)                      as col_float_array_null,\n" +
                "        [[1.25,2.5],null]::array(array(float4))  as col_float_array_null_array,\n" +
                "        [[1.25,null],null]::array(array(float4)) as col_float_null_array_null_array,\n" +
                "        null::array(array(float4))               as col_float_array_array_null,\n" +
                // double
                "        1.23456789012                             as col_double,\n" +
                "        null::double                              as col_double_null,\n" +
                "        [1.125,2.25]::array(double)               as col_double_array,\n" +
                "        [1.125,null]::array(double)               as col_double_null_array,\n" +
                "        null::array(double)                       as col_double_array_null,\n" +
                "        [[1.125,2.25],null]::array(array(double)) as col_double_array_null_array,\n" +
                "        [[1.125,null],null]::array(array(double)) as col_double_null_array_null_array,\n" +
                "        null::array(array(double))                as col_double_array_array_null,\n" +
                // text
                "        'text'                                 as col_text,\n" +
                "        null::text                             as col_text_null,\n" +
                "        ['a','b']::array(text)                 as col_text_array,\n" +
                "        ['a',null]::array(text)                as col_text_null_array,\n" +
                "        null::array(text)                      as col_text_array_null,\n" +
                "        [['a','b'],null]::array(array(text))   as col_text_array_null_array,\n" +
                "        [['a',null],null]::array(array(text))  as col_text_null_array_null_array,\n" +
                "        null::array(array(text))               as col_text_array_array_null,\n" +
                // date
                "        '2021-03-28'::date                                     as col_date,\n" +
                "        null::date                                             as col_date_null,\n" +
                "        ['2021-03-28','2021-03-29']::array(date)               as col_date_array,\n" +
                "        ['2021-03-28',null]::array(date)                       as col_date_null_array,\n" +
                "        null::array(date)                                      as col_date_array_null,\n" +
                "        [['2021-03-28','2021-03-29'],null]::array(array(date)) as col_date_array_null_array,\n" +
                "        [['2021-03-28','2021-03-29']]::array(array(date))      as col_date_array_array,\n" +
                "        [['2021-03-28',null],null]::array(array(date))         as col_date_null_array_null_array,\n" +
                "        null::array(array(date))                               as col_date_array_array_null,\n" +
                // timestamp
                "        '2019-07-31 01:01:01'::timestamp                                              as col_timestamp,\n" +
                "        null::timestamp                                                               as col_timestamp_null,\n" +
                "        ['2019-07-31 01:01:01','2019-08-01 00:00:00']::array(timestamp)               as col_timestamp_array,\n" +
                "        ['2019-07-31 01:01:01',null]::array(timestamp)                                as col_timestamp_null_array,\n" +
                "        null::array(timestamp)                                                        as col_timestamp_array_null,\n" +
                "        [['2019-07-31 01:01:01','2019-08-01 00:00:00'],null]::array(array(timestamp)) as col_timestamp_array_null_array,\n" +
                "        [['2019-07-31 01:01:01','2019-08-01 00:00:00']]::array(array(timestamp))      as col_timestamp_array_null_array,\n" +
                "        [['2019-07-31 01:01:01',null],null]::array(array(timestamp))                  as col_timestamp_null_array_null_array,\n" +
                "        null::array(array(timestamp))                                                 as col_timestamp_array_array_null,\n" +
                // timestamptz
                "        '1111-01-05 17:04:42.123456+00'::timestamptz                                                        as col_timestamptz,\n" +
                "        null::timestamptz                                                                                   as col_timestamptz_null,\n" +
                "        ['1111-01-05 17:04:42.123456+00','1111-01-06 17:04:42.123456+00']::array(timestamptz)               as col_timestamptz_array,\n" +
                "        ['1111-01-05 17:04:42.123456+00',null]::array(timestamptz)                                          as col_timestamptz_null_array,\n" +
                "        null::array(timestamptz)                                                                            as col_timestamptz_array_null,\n" +
                "        [['1111-01-05 17:04:42.123456+00','1111-01-06 17:04:42.123456+00'],null]::array(array(timestamptz)) as col_timestamptz_array_null_array,\n" +
                "        [['1111-01-05 17:04:42.123456+00','1111-01-06 17:04:42.123456+00']]::array(array(timestamptz))      as col_timestamptz_array_null_array,\n" +
                "        [['1111-01-05 17:04:42.123456+00',null],null]::array(array(timestamptz))                            as col_timestamptz_null_array_null_array,\n" +
                "        null::array(array(timestamptz))                                                                     as col_timestamptz_array_array_null,\n" +
                // boolean
                "        true                                    as col_boolean,\n" +
                "        null::bool                              as col_boolean_null,\n" +
                "        [true,false]::array(bool)               as col_boolean_array,\n" +
                "        [true,null]::array(bool)                as col_boolean_null_array,\n" +
                "        null::array(bool)                       as col_boolean_array_null,\n" +
                "        [[true,false],null]::array(array(bool)) as col_boolean_array_null_array,\n" +
                "        [[true,null],null]::array(array(bool))  as col_boolean_null_array_null_array,\n" +
                "        null::array(array(bool))                as col_boolean_array_array_null,\n" +
                // decimal
                "        '1231232.123459999990457054844258706536'::decimal(38, 30) as col_decimal,\n" +
                "        null::decimal(38, 30)                                     as col_decimal_null,\n" +
                "        ['1.50','-2.25']::array(decimal(38, 30))                  as col_decimal_array,\n" +
                "        ['1.50',null]::array(decimal(38, 30))                     as col_decimal_null_array,\n" +
                "        null::array(decimal(38, 30))                              as col_decimal_array_null,\n" +
                "        [['1.50','-2.25'],null]::array(array(decimal(38, 30)))    as col_decimal_array_null_array,\n" +
                "        [['1.50','-2.25']]::array(array(decimal(38, 30)))         as col_decimal_array_null_array,\n" +
                "        [['1.50',null],null]::array(array(decimal(38, 30)))       as col_decimal_null_array_null_array,\n" +
                "        null::array(array(decimal(38, 30)))                       as col_decimal_array_array_null,\n" +
                // bytea
                "        'abc123'::bytea                       as col_bytea,\n" +
                "        null::bytea                           as col_bytea_null,\n" +
                // geography
                "        'point(1 2)'::geography                                     as col_geography,\n" +
                "        null::geography                                             as col_geography_null,\n" +
                "        ['point(1 2)','point(2 3)']::array(geography)               as col_geography_array,\n" +
                "        ['point(1 2)',null]::array(geography)                       as col_geography_null_array,\n" +
                "        null::array(geography)                                      as col_geography_array_null,\n" +
                "        [['point(1 2)','point(2 3)'],null]::array(array(geography)) as col_geography_array_null_array,\n" +
                "        [['point(1 2)',null],null]::array(array(geography))         as col_geography_null_array_null_array,\n" +
                "        null::array(array(geography))                               as col_geography_array_array_null";

        private static readonly List<Type> TypeList = new()
        {
            // int
            typeof(int), typeof(int), typeof(int[]), typeof(int?[]), typeof(int[]), typeof(int[][]), typeof(int?[][]), typeof(int[][]),
            // long
            typeof(long), typeof(long), typeof(long[]), typeof(long?[]), typeof(long[]), typeof(long[][]), typeof(long?[][]), typeof(long[][]),
            // float
            typeof(float), typeof(float), typeof(float[]), typeof(float?[]), typeof(float[]), typeof(float[][]), typeof(float?[][]), typeof(float[][]),
            // double
            typeof(double), typeof(double), typeof(double[]), typeof(double?[]), typeof(double[]), typeof(double[][]), typeof(double?[][]), typeof(double[][]),
            // text
            typeof(string), typeof(string), typeof(string[]), typeof(string[]), typeof(string[]), typeof(string[][]), typeof(string[][]), typeof(string[][]),
            // date
            typeof(DateTime), typeof(DateTime), typeof(DateTime[]), typeof(DateTime?[]), typeof(DateTime[]), typeof(DateTime?[][]), typeof(DateTime[][]), typeof(DateTime?[][]), typeof(DateTime[][]),
            // timestamp
            typeof(DateTime), typeof(DateTime), typeof(DateTime[]), typeof(DateTime?[]), typeof(DateTime[]), typeof(DateTime?[][]), typeof(DateTime[][]), typeof(DateTime?[][]), typeof(DateTime[][]),
            // timestamptz
            typeof(DateTime), typeof(DateTime), typeof(DateTime[]), typeof(DateTime?[]), typeof(DateTime[]), typeof(DateTime?[][]), typeof(DateTime[][]), typeof(DateTime?[][]), typeof(DateTime[][]),
            // boolean
            typeof(bool), typeof(bool), typeof(bool[]), typeof(bool?[]), typeof(bool[]), typeof(bool[][]), typeof(bool?[][]), typeof(bool[][]),
            // decimal
            typeof(decimal), typeof(decimal), typeof(decimal[]), typeof(decimal?[]), typeof(decimal[]), typeof(decimal?[][]), typeof(decimal[][]), typeof(decimal?[][]), typeof(decimal[][]),
            // bytea
            typeof(byte[]), typeof(byte[]),
            // geography
            typeof(string), typeof(string), typeof(string[]), typeof(string[]), typeof(string[]), typeof(string[][]), typeof(string[][]), typeof(string[][])
        };

        private static readonly object?[] ExpectedValues = {
            // int
            1, null,
            new[] { 1, 2 },
            new int?[] { 1, null },
            null,
            new[] { new[] { 1, 2 }, null },
            new[] { new int?[] { 1, null }, null },
            null,
            // long
            30000000000L, null,
            new[] { 1L, 2L, 3L },
            new long?[] { 1L, null },
            null,
            new[] { new[] { 1L, 2L }, null },
            new[] { new long?[] { 1L, null }, null },
            null,
            // float
            1.23f, null,
            new[] { 1.25f, 2.5f },
            new float?[] { 1.25f, null },
            null,
            new[] { new[] { 1.25f, 2.5f }, null },
            new[] { new float?[] { 1.25f, null }, null },
            null,
            // double
            1.23456789012d, null,
            new[] { 1.125d, 2.25d },
            new double?[] { 1.125d, null },
            null,
            new[] { new[] { 1.125d, 2.25d }, null },
            new[] { new double?[] { 1.125d, null }, null },
            null,
            // text
            "text", null,
            new[] { "a", "b" },
            new[] { "a", null },
            null,
            new[] { new[] { "a", "b" }, null },
            new[] { new[] { "a", null }, null },
            null,
            // date
            Parse("2021-03-28"), null,
            new[] { Parse("2021-03-28"), Parse("2021-03-29") },
            new DateTime?[] { Parse("2021-03-28"), null },
            null,
            new[] { new[] { Parse("2021-03-28"), Parse("2021-03-29") }, null },
            new[] { new[] { Parse("2021-03-28"), Parse("2021-03-29") } },
            new[] { new DateTime?[] { Parse("2021-03-28"), null }, null },
            null,
            // timestamp
            Parse("2019-07-31 01:01:01"), null,
            new[] { Parse("2019-07-31 01:01:01"), Parse("2019-08-01 00:00:00") },
            new DateTime?[] { Parse("2019-07-31 01:01:01"), null },
            null,
            new[] { new[] { Parse("2019-07-31 01:01:01"), Parse("2019-08-01 00:00:00") }, null },
            new[] { new[] { Parse("2019-07-31 01:01:01"), Parse("2019-08-01 00:00:00") } },
            new[] { new DateTime?[] { Parse("2019-07-31 01:01:01"), null }, null },
            null,
            // timestamptz
            Parse("1111-01-05 17:04:42.123456+00"), null,
            new[] { Parse("1111-01-05 17:04:42.123456+00"), Parse("1111-01-06 17:04:42.123456+00") },
            new DateTime?[] { Parse("1111-01-05 17:04:42.123456+00"), null },
            null,
            new[] { new[] { Parse("1111-01-05 17:04:42.123456+00"), Parse("1111-01-06 17:04:42.123456+00") }, null },
            new[] { new[] { Parse("1111-01-05 17:04:42.123456+00"), Parse("1111-01-06 17:04:42.123456+00") } },
            new[] { new DateTime?[] { Parse("1111-01-05 17:04:42.123456+00"), null }, null },
            null,
            // boolean
            true, null,
            new[] { true, false },
            new bool?[] { true, null },
            null,
            new[] { new[] { true, false }, null },
            new[] { new bool?[] { true, null }, null },
            null,
            // decimal
            decimal.Parse("1231232.123459999990457054844258706536"), null,
            new[] { decimal.Parse("1.50"), decimal.Parse("-2.25") },
            new decimal?[] { decimal.Parse("1.50"), null },
            null,
            new[] { new[] { decimal.Parse("1.50"), decimal.Parse("-2.25") }, null },
            new[] { new[] { decimal.Parse("1.50"), decimal.Parse("-2.25") } },
            new[] { new decimal?[] { decimal.Parse("1.50"), null }, null },
            null,
            // bytea
            Encoding.ASCII.GetBytes("abc123"), null,
            // geography
            "0101000020E6100000FEFFFFFFFFFFEF3F0000000000000040", null,
            new[] { "0101000020E6100000FEFFFFFFFFFFEF3F0000000000000040", "0101000020E6100000FEFFFFFFFFFFFF3F0100000000000840" },
            new[] { "0101000020E6100000FEFFFFFFFFFFEF3F0000000000000040", null },
            null,
            new[] { new[] { "0101000020E6100000FEFFFFFFFFFFEF3F0000000000000040", "0101000020E6100000FEFFFFFFFFFFFF3F0100000000000840" }, null },
            new[] { new[] { "0101000020E6100000FEFFFFFFFFFFEF3F0000000000000040", null }, null },
            null
        };

        [Test]
        [Category("engine-v2")]
        public async Task ExecuteReaderAsyncWithDifferentDataTypes()
        {
            await using var connection = new FireboltConnection(UserConnectionString);
            await connection.OpenAsync();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = VariousTypeQuery;

            await using var reader = await command.ExecuteReaderAsync();
            Assert.That(await reader.ReadAsync(), Is.EqualTo(true));
            for (var i = 0; i < TypeList.Count; i++)
            {
                Assert.That(reader.GetFieldType(i), Is.EqualTo(TypeList[i]));
            }
            for (var i = 0; i < ExpectedValues.Length; i++)
            {
                if (ExpectedValues[i] == null)
                {
                    Assert.That(reader.IsDBNull(i), Is.True, $"Column at index {i} expected to be null");
                }
                else
                {
                    var value = reader.GetValue(i);
                    Assert.That(value, Is.EqualTo(ExpectedValues[i]), $"Mismatch at column index {i}");
                }
            }
        }

        [Test]
        [Category("engine-v2")]
        public void ExecuteReaderWithDifferentDataTypes()
        {
            using var connection = new FireboltConnection(UserConnectionString);
            connection.Open();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = VariousTypeQuery;

            using var reader = command.ExecuteReader();
            Assert.That(reader.Read(), Is.EqualTo(true));
            for (var i = 0; i < TypeList.Count; i++)
            {
                Assert.That(reader.GetFieldType(i), Is.EqualTo(TypeList[i]));
            }
            for (var i = 0; i < ExpectedValues.Length; i++)
            {
                if (ExpectedValues[i] == null)
                {
                    Assert.That(reader.IsDBNull(i), Is.True, $"Column at index {i} expected to be null");
                }
                else
                {
                    var value = reader.GetValue(i);
                    Assert.That(value, Is.EqualTo(ExpectedValues[i]), $"Mismatch at column index {i}");
                }
            }
        }

        [Test]
        [Category("engine-v2")]
        public void AdapterVariousValuesTest()
        {
            DbDataAdapter adapter = new FireboltDataAdapter(VariousTypeQuery, UserConnectionString);
            var dataSet = new DataSet();

            adapter.Fill(dataSet);
            var table = dataSet.Tables[0];

            for (var i = 0; i < TypeList.Count; i++)
            {
                Assert.That(table.Columns[i].DataType, Is.EqualTo(TypeList[i]));
            }
            var row = table.Rows[0];
            for (var i = 0; i < ExpectedValues.Length; i++)
            {
                if (ExpectedValues[i] == null)
                {
                    Assert.That(row.IsNull(i), Is.True, $"Column at index {i} expected to be null");
                }
                else
                {
                    var value = row[i];
                    Assert.That(value, Is.EqualTo(ExpectedValues[i]), $"Mismatch at column index {i}");
                }
            }
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
    }
}
