using System.Data.Common;
using System.Text;
using FireboltDotNetSdk.Client;
using static System.DateTime;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    internal class DataTypesTestV1 : IntegrationTest
    {
        private static readonly string UserConnectionString = ConnectionString();
        private const string VariousTypeQuery =
            "select  1                                     as col_int,\n" +
            "        [1,2]                                 as col_int_array,\n" +
            "        [[1,2],[]]::array(array(int))         as col_int_array_null_array,\n" +
            "        30000000000                            as col_long,\n" +
            "        [1,2,3]::array(bigint)                 as col_long_array,\n" +
            "        [[1,2],[]]::array(array(bigint))       as col_long_array_null_array,\n" +
            "        1.23::float4                             as col_float,\n" +
            "        [1.25,2.5]::array(float4)                as col_float_array,\n" +
            "        [[1.25,2.5],[]]::array(array(float4))    as col_float_array_null_array,\n" +
            "        1.23456789012                             as col_double,\n" +
            "        [1.125,2.25]::array(double)               as col_double_array,\n" +
            "        [[1.125,2.25],[]]::array(array(double))   as col_double_array_null_array,\n" +
            "        'text'                                 as col_text,\n" +
            "        ['a','b']::array(text)                 as col_text_array,\n" +
            "        [['a','b'],[]]::array(array(text))     as col_text_array_null_array,\n" +
            "        '2021-03-28'::date                                        as col_date,\n" +
            "        ['2021-03-28','2021-03-29']::array(date)                  as col_date_array,\n" +
            "        [['2021-03-28','2021-03-29'],[]]::array(array(date))      as col_date_array_array,\n" +
            "        '2019-07-31 01:01:01'::timestamp                                                 as col_timestamp,\n" +
            "        ['2019-07-31 01:01:01','2019-08-01 00:00:00']::array(timestamp)                  as col_timestamp_array,\n" +
            "        [['2019-07-31 01:01:01','2019-08-01 00:00:00'],[]]::array(array(timestamp))      as col_timestamp_array_array,\n" +
            "        '1111-01-05 17:04:42.123456+00'::timestamptz                                                           as col_timestamptz,\n" +
            "        ['1111-01-05 17:04:42.123456+00','1111-01-06 17:04:42.123456+00']::array(timestamptz)                  as col_timestamptz_array,\n" +
            "        [['1111-01-05 17:04:42.123456+00','1111-01-06 17:04:42.123456+00'],[]]::array(array(timestamptz))      as col_timestamptz_array_array,\n" +
            "        true                                    as col_boolean,\n" +
            "        [true,false]::array(bool)               as col_boolean_array,\n" +
            "        [[true,false],[]]::array(array(bool))   as col_boolean_array_null_array,\n" +
            "        '1231232.12346'::decimal(38, 30)    as col_decimal,\n" +
            "        ['1.50','-2.25']::array(decimal(38, 30))                     as col_decimal_array,\n" +
            "        [['1.50','-2.25'],[]]::array(array(decimal(38, 30)))         as col_decimal_array_array,\n" +
            "        'abc123'::bytea                                    as col_bytea,\n";

        private static readonly List<Type> TypeList = new()
        {
            // int
            typeof(int), typeof(int[]), typeof(int[][]),
            // long
            typeof(long), typeof(long[]), typeof(long[][]),
            // float
            typeof(float), typeof(float[]), typeof(float[][]),
            // double
            typeof(double), typeof(double[]), typeof(double[][]),
            // text
            typeof(string), typeof(string[]), typeof(string[][]),
            // date
            typeof(DateTime), typeof(DateTime[]), typeof(DateTime[][]),
            // timestamp
            typeof(DateTime), typeof(DateTime[]), typeof(DateTime[][]),
            // timestamptz
            typeof(DateTime), typeof(DateTime[]), typeof(DateTime[][]),
            // boolean
            typeof(bool), typeof(bool[]), typeof(bool[][]),
            // decimal
            typeof(decimal), typeof(decimal[]), typeof(decimal[][]),
            // bytea
            typeof(byte[])
        };

        private static readonly object?[] ExpectedValues = {
            // int
            1,
            new[] { 1, 2 },
            new[] { new[] { 1, 2 }, Array.Empty<int>() },
            // long
            30000000000L,
            new[] { 1L, 2L, 3L },
            new[] { new[] { 1L, 2L }, Array.Empty<long>() },
            // float
            1.23f,
            new[] { 1.25f, 2.5f },
            new[] { new[] { 1.25f, 2.5f }, Array.Empty<float>() },
            // double
            1.23456789012d,
            new[] { 1.125d, 2.25d },
            new[] { new[] { 1.125d, 2.25d }, Array.Empty<double>() },
            // text
            "text",
            new[] { "a", "b" },
            new[] { new[] { "a", "b" }, Array.Empty<string>() },
            // date
            Parse("2021-03-28"),
            new[] { Parse("2021-03-28"), Parse("2021-03-29") },
            new[] { new[] { Parse("2021-03-28"), Parse("2021-03-29") }, Array.Empty<DateTime>() },
            // timestamp
            Parse("2019-07-31 01:01:01"),
            new[] { Parse("2019-07-31 01:01:01"), Parse("2019-08-01 00:00:00") },
            new[] { new[] { Parse("2019-07-31 01:01:01"), Parse("2019-08-01 00:00:00") }, Array.Empty<DateTime>() },
            // timestamptz
            Parse("1111-01-05 17:04:42.123456+00"),
            new[] { Parse("1111-01-05 17:04:42.123456+00"), Parse("1111-01-06 17:04:42.123456+00") },
            new[] { new[] { Parse("1111-01-05 17:04:42.123456+00"), Parse("1111-01-06 17:04:42.123456+00") }, Array.Empty<DateTime>() },
            // boolean
            true,
            new[] { true, false },
            new[] { new[] { true, false }, Array.Empty<bool>() },
            // decimal
            decimal.Parse("1231232.12346"),
            new[] { decimal.Parse("1.50"), decimal.Parse("-2.25") },
            new[] { new[] { decimal.Parse("1.50"), decimal.Parse("-2.25") }, Array.Empty<decimal>() },
            // bytea
            Encoding.ASCII.GetBytes("abc123"),
        };

        [Test]
        [Category("v1")]
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
            VerifyReturnedValues(reader);
        }

        [Test]
        [Category("v1")]
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
            VerifyReturnedValues(reader);
        }

        private static void VerifyReturnedValues(DbDataReader reader)
        {
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
    }
}