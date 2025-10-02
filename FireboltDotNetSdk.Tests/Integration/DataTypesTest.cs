using System.Data;
using System.Data.Common;
using System.Text;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Utils;
using static System.DateTime;
using System.Reflection;
using System.Text.RegularExpressions;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    internal class DataTypesTest : IntegrationTest
    {
        private static readonly string UserConnectionString = ConnectionString();
        private const string StructHelperTable = "struct_various_types_helper";
        private const string StructTable = "struct_various";
        private static List<string> StructAliases = new();
        // Legacy combined test retained for compatibility; specific matrix tests are parameterized below
        // arrays of arrays test data contain [[1,2],[]] instead of [[1,2],null] because the query result was inconsistent when it came to the base type being null or not
        // so for consistency we use empty array instead of null for the second element so that the base type is always non-nullable
        private const string VariousTypeQuery =
                "select  1                                     as col_int,\n" +
                "        null::int                             as col_int_null,\n" +
                "        [1,2]                                 as col_int_array,\n" +
                "        [1,null]                              as col_int_null_array,\n" +
                "        null::array(int)                      as col_int_array_null,\n" +
                "        [[1,2],[]]::array(array(int))         as col_int_array_null_array,\n" +
                "        [[1,null],null]::array(array(int))    as col_int_null_array_null_array,\n" +
                "        null::array(array(int))               as col_int_array_array_null,\n" +
                "        30000000000                            as col_long,\n" +
                "        null::bigint                           as col_long_null,\n" +
                "        [1,2,3]::array(bigint)                 as col_long_array,\n" +
                "        [1,null]::array(bigint)                as col_long_null_array,\n" +
                "        null::array(bigint)                    as col_long_array_null,\n" +
                "        [[1,2],[]]::array(array(bigint))       as col_long_array_null_array,\n" +
                "        [[1,null],null]::array(array(bigint))  as col_long_null_array_null_array,\n" +
                "        null::array(array(bigint))             as col_long_array_array_null,\n" +
                "        1.23::float4                             as col_float,\n" +
                "        null::float4                             as col_float_null,\n" +
                "        [1.25,2.5]::array(float4)                as col_float_array,\n" +
                "        [1.25,null]::array(float4)               as col_float_null_array,\n" +
                "        null::array(float4)                      as col_float_array_null,\n" +
                "        [[1.25,2.5],[]]::array(array(float4))    as col_float_array_null_array,\n" +
                "        [[1.25,null],null]::array(array(float4)) as col_float_null_array_null_array,\n" +
                "        null::array(array(float4))               as col_float_array_array_null,\n" +
                "        1.23456789012                             as col_double,\n" +
                "        null::double                              as col_double_null,\n" +
                "        [1.125,2.25]::array(double)               as col_double_array,\n" +
                "        [1.125,null]::array(double)               as col_double_null_array,\n" +
                "        null::array(double)                       as col_double_array_null,\n" +
                "        [[1.125,2.25],[]]::array(array(double))   as col_double_array_null_array,\n" +
                "        [[1.125,null],null]::array(array(double)) as col_double_null_array_null_array,\n" +
                "        null::array(array(double))                as col_double_array_array_null,\n" +
                "        'text'                                 as col_text,\n" +
                "        null::text                             as col_text_null,\n" +
                "        ['a','b']::array(text)                 as col_text_array,\n" +
                "        ['a',null]::array(text)                as col_text_null_array,\n" +
                "        null::array(text)                      as col_text_array_null,\n" +
                "        [['a','b'],[]]::array(array(text))     as col_text_array_null_array,\n" +
                "        [['a',null],null]::array(array(text))  as col_text_null_array_null_array,\n" +
                "        null::array(array(text))               as col_text_array_array_null,\n" +
                "        '2021-03-28'::date                                        as col_date,\n" +
                "        null::date                                                as col_date_null,\n" +
                "        ['2021-03-28','2021-03-29']::array(date)                  as col_date_array,\n" +
                "        ['2021-03-28',null]::array(date)                          as col_date_null_array,\n" +
                "        null::array(date)                                         as col_date_array_null,\n" +
                "        [['2021-03-28','2021-03-29'],null]::array(array(date))    as col_date_array_null_array,\n" +
                "        [['2021-03-28','2021-03-29'],[]]::array(array(date))      as col_date_array_array,\n" +
                "        [['2021-03-28',null],null]::array(array(date))            as col_date_null_array_null_array,\n" +
                "        null::array(array(date))                                  as col_date_array_array_null,\n" +
                "        '2019-07-31 01:01:01'::timestamp                                                 as col_timestamp,\n" +
                "        null::timestamp                                                                  as col_timestamp_null,\n" +
                "        ['2019-07-31 01:01:01','2019-08-01 00:00:00']::array(timestamp)                  as col_timestamp_array,\n" +
                "        ['2019-07-31 01:01:01',null]::array(timestamp)                                   as col_timestamp_null_array,\n" +
                "        null::array(timestamp)                                                           as col_timestamp_array_null,\n" +
                "        [['2019-07-31 01:01:01','2019-08-01 00:00:00'],null]::array(array(timestamp))    as col_timestamp_array_null_array,\n" +
                "        [['2019-07-31 01:01:01','2019-08-01 00:00:00'],[]]::array(array(timestamp))      as col_timestamp_array_array,\n" +
                "        [['2019-07-31 01:01:01',null],null]::array(array(timestamp))                     as col_timestamp_null_array_null_array,\n" +
                "        null::array(array(timestamp))                                                    as col_timestamp_array_array_null,\n" +
                "        '1111-01-05 17:04:42.123456+00'::timestamptz                                                           as col_timestamptz,\n" +
                "        null::timestamptz                                                                                      as col_timestamptz_null,\n" +
                "        ['1111-01-05 17:04:42.123456+00','1111-01-06 17:04:42.123456+00']::array(timestamptz)                  as col_timestamptz_array,\n" +
                "        ['1111-01-05 17:04:42.123456+00',null]::array(timestamptz)                                             as col_timestamptz_null_array,\n" +
                "        null::array(timestamptz)                                                                               as col_timestamptz_array_null,\n" +
                "        [['1111-01-05 17:04:42.123456+00','1111-01-06 17:04:42.123456+00'],null]::array(array(timestamptz))    as col_timestamptz_array_null_array,\n" +
                "        [['1111-01-05 17:04:42.123456+00','1111-01-06 17:04:42.123456+00'],[]]::array(array(timestamptz))      as col_timestamptz_array_array,\n" +
                "        [['1111-01-05 17:04:42.123456+00',null],null]::array(array(timestamptz))                               as col_timestamptz_null_array_null_array,\n" +
                "        null::array(array(timestamptz))                                                                        as col_timestamptz_array_array_null,\n" +
                "        true                                    as col_boolean,\n" +
                "        null::bool                              as col_boolean_null,\n" +
                "        [true,false]::array(bool)               as col_boolean_array,\n" +
                "        [true,null]::array(bool)                as col_boolean_null_array,\n" +
                "        null::array(bool)                       as col_boolean_array_null,\n" +
                "        [[true,false],[]]::array(array(bool))   as col_boolean_array_null_array,\n" +
                "        [[true,null],null]::array(array(bool))  as col_boolean_null_array_null_array,\n" +
                "        null::array(array(bool))                as col_boolean_array_array_null,\n" +
                "        '1231232.123459999990457054844258706536'::decimal(38, 30)    as col_decimal,\n" +
                "        null::decimal(38, 30)                                        as col_decimal_null,\n" +
                "        ['1.50','-2.25']::array(decimal(38, 30))                     as col_decimal_array,\n" +
                "        ['1.50',null]::array(decimal(38, 30))                        as col_decimal_null_array,\n" +
                "        null::array(decimal(38, 30))                                 as col_decimal_array_null,\n" +
                "        [['1.50','-2.25'],null]::array(array(decimal(38, 30)))       as col_decimal_array_null_array,\n" +
                "        [['1.50','-2.25'],[]]::array(array(decimal(38, 30)))         as col_decimal_array_array,\n" +
                "        [['1.50',null],null]::array(array(decimal(38, 30)))          as col_decimal_null_array_null_array,\n" +
                "        null::array(array(decimal(38, 30)))                          as col_decimal_array_array_null,\n" +
                "        'abc123'::bytea                                    as col_bytea,\n" +
                "        null::bytea                                        as col_bytea_null,\n" +
                "        'point(1 2)'::geography                                     as col_geography,\n" +
                "        null::geography                                             as col_geography_null,\n" +
                "        ['point(1 2)','point(2 3)']::array(geography)               as col_geography_array,\n" +
                "        ['point(1 2)',null]::array(geography)                       as col_geography_null_array,\n" +
                "        null::array(geography)                                      as col_geography_array_null,\n" +
                "        [['point(1 2)','point(2 3)'],[]]::array(array(geography))   as col_geography_array_null_array,\n" +
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
            new[] { new[] { 1, 2 }, Array.Empty<int>() },
            new[] { new int?[] { 1, null }, null },
            null,
            // long
            30000000000L, null,
            new[] { 1L, 2L, 3L },
            new long?[] { 1L, null },
            null,
            new[] { new[] { 1L, 2L }, Array.Empty<long>() },
            new[] { new long?[] { 1L, null }, null },
            null,
            // float
            1.23f, null,
            new[] { 1.25f, 2.5f },
            new float?[] { 1.25f, null },
            null,
            new[] { new[] { 1.25f, 2.5f }, Array.Empty<float>() },
            new[] { new float?[] { 1.25f, null }, null },
            null,
            // double
            1.23456789012d, null,
            new[] { 1.125d, 2.25d },
            new double?[] { 1.125d, null },
            null,
            new[] { new[] { 1.125d, 2.25d }, Array.Empty<double>() },
            new[] { new double?[] { 1.125d, null }, null },
            null,
            // text
            "text", null,
            new[] { "a", "b" },
            new[] { "a", null },
            null,
            new[] { new[] { "a", "b" }, Array.Empty<string>() },
            new[] { new[] { "a", null }, null },
            null,
            // date
            Parse("2021-03-28"), null,
            new[] { Parse("2021-03-28"), Parse("2021-03-29") },
            new DateTime?[] { Parse("2021-03-28"), null },
            null,
            new[] { new[] { Parse("2021-03-28"), Parse("2021-03-29") }, null },
            new[] { new[] { Parse("2021-03-28"), Parse("2021-03-29") }, Array.Empty<DateTime>() },
            new[] { new DateTime?[] { Parse("2021-03-28"), null }, null },
            null,
            // timestamp
            Parse("2019-07-31 01:01:01"), null,
            new[] { Parse("2019-07-31 01:01:01"), Parse("2019-08-01 00:00:00") },
            new DateTime?[] { Parse("2019-07-31 01:01:01"), null },
            null,
            new[] { new[] { Parse("2019-07-31 01:01:01"), Parse("2019-08-01 00:00:00") }, null },
            new[] { new[] { Parse("2019-07-31 01:01:01"), Parse("2019-08-01 00:00:00") }, Array.Empty<DateTime>() },
            new[] { new DateTime?[] { Parse("2019-07-31 01:01:01"), null }, null },
            null,
            // timestamptz
            Parse("1111-01-05 17:04:42.123456+00"), null,
            new[] { Parse("1111-01-05 17:04:42.123456+00"), Parse("1111-01-06 17:04:42.123456+00") },
            new DateTime?[] { Parse("1111-01-05 17:04:42.123456+00"), null },
            null,
            new[] { new[] { Parse("1111-01-05 17:04:42.123456+00"), Parse("1111-01-06 17:04:42.123456+00") }, null },
            new[] { new[] { Parse("1111-01-05 17:04:42.123456+00"), Parse("1111-01-06 17:04:42.123456+00") }, Array.Empty<DateTime>() },
            new[] { new DateTime?[] { Parse("1111-01-05 17:04:42.123456+00"), null }, null },
            null,
            // boolean
            true, null,
            new[] { true, false },
            new bool?[] { true, null },
            null,
            new[] { new[] { true, false }, Array.Empty<bool>() },
            new[] { new bool?[] { true, null }, null },
            null,
            // decimal
            decimal.Parse("1231232.123459999990457054844258706536"), null,
            new[] { decimal.Parse("1.50"), decimal.Parse("-2.25") },
            new decimal?[] { decimal.Parse("1.50"), null },
            null,
            new[] { new[] { decimal.Parse("1.50"), decimal.Parse("-2.25") }, null },
            new[] { new[] { decimal.Parse("1.50"), decimal.Parse("-2.25") }, Array.Empty<decimal>() },
            new[] { new decimal?[] { decimal.Parse("1.50"), null }, null },
            null,
            // bytea
            Encoding.ASCII.GetBytes("abc123"), null,
            // geography
            "0101000020E6100000FEFFFFFFFFFFEF3F0000000000000040", null,
            new[] { "0101000020E6100000FEFFFFFFFFFFEF3F0000000000000040", "0101000020E6100000FEFFFFFFFFFFFF3F0100000000000840" },
            new[] { "0101000020E6100000FEFFFFFFFFFFEF3F0000000000000040", null },
            null,
            new[] { new[] { "0101000020E6100000FEFFFFFFFFFFEF3F0000000000000040", "0101000020E6100000FEFFFFFFFFFFFF3F0100000000000840" }, Array.Empty<string>() },
            new[] { new[] { "0101000020E6100000FEFFFFFFFFFFEF3F0000000000000040", null }, null },
            null
        };

        [OneTimeSetUp]
        public void OneTimeSetup_StructVarious()
        {
            using var connection = new FireboltConnection(UserConnectionString);
            connection.Open();

            var setup = new[]
            {
                "SET advanced_mode=1",
                "SET enable_create_table_v2=true",
                "SET enable_struct_syntax=true",
                "SET prevent_create_on_information_schema=true",
                "SET enable_create_table_with_struct_type=true",
                $"DROP TABLE IF EXISTS {StructHelperTable}",
                $"DROP TABLE IF EXISTS {StructTable}"
            };
            foreach (var s in setup)
            {
                var c = (FireboltCommand)connection.CreateCommand();
                c.CommandText = s;
                c.ExecuteNonQuery();
            }

            var createHelper = (FireboltCommand)connection.CreateCommand();
            createHelper.CommandText = $"CREATE TABLE {StructHelperTable} AS {VariousTypeQuery}";
            createHelper.ExecuteNonQuery();

            StructAliases = Regex.Matches(VariousTypeQuery, @"as\s+([a-zA-Z_]*)")
                .Select(m => m.Groups[1].Value)
                .ToList();

            var typesCmd = (FireboltCommand)connection.CreateCommand();
            typesCmd.CommandText = $"SELECT column_name, data_type FROM information_schema.columns WHERE table_name='{StructHelperTable}' ORDER BY ordinal_position";
            var typeMap = new Dictionary<string, string>();
            using (var tReader = typesCmd.ExecuteReader())
            {
                while (tReader.Read())
                {
                    var col = tReader.GetString(0);
                    var typ = tReader.GetString(1);
                    typeMap[col] = typ;
                }
            }

            var structTypeDef = string.Join(", ", StructAliases.Select(a => $"{a} {typeMap[a]}"));

            var createStruct = (FireboltCommand)connection.CreateCommand();
            createStruct.CommandText = $"CREATE TABLE {StructTable} (s struct({structTypeDef}))";
            createStruct.ExecuteNonQuery();

            var structExpr = string.Join(", ", StructAliases);
            var insertStruct = (FireboltCommand)connection.CreateCommand();
            insertStruct.CommandText = $"INSERT INTO {StructTable} SELECT struct({structExpr}) FROM {StructHelperTable}";
            insertStruct.ExecuteNonQuery();
        }

        [OneTimeTearDown]
        public void OneTimeTearDown_StructVarious()
        {
            using var connection = new FireboltConnection(UserConnectionString);
            connection.Open();
            foreach (var s in new[] { $"DROP TABLE IF EXISTS {StructTable}", $"DROP TABLE IF EXISTS {StructHelperTable}" })
            {
                var c = (FireboltCommand)connection.CreateCommand();
                c.CommandText = s;
                c.ExecuteNonQuery();
            }
        }


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
            VerifyReturnedValues(reader);
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
            VerifyReturnedValues(reader);
        }

        [Test]
        [Category("engine-v2")]
        public void ExecuteReaderWithDifferentDataTypes_ColumnSchema()
        {
            using var connection = new FireboltConnection(UserConnectionString);
            connection.Open();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = VariousTypeQuery;

            using var reader = command.ExecuteReader();
            Assert.That(reader.Read(), Is.True);

            var schema = reader.GetColumnSchema();
            Assert.That(schema, Has.Count.EqualTo(TypeList.Count));

            for (var i = 0; i < schema.Count; i++)
            {
                Assert.Multiple(() =>
                {
                    Assert.That(schema[i].ColumnName, Is.EqualTo(StructAliases[i]));
                    Assert.That(schema[i].ColumnOrdinal, Is.EqualTo(i));
                    Assert.That(schema[i].DataType, Is.EqualTo(TypeList[i]));
                    Assert.That(schema[i].ColumnSize, Is.EqualTo(-1));
                    Assert.That(schema[i].DataTypeName, Is.EqualTo(reader.GetDataTypeName(i)));
                    Assert.That(schema[i].AllowDBNull, Is.EqualTo(reader.IsDBNull(i)));
                    Assert.That(schema[i].NumericPrecision, Is.EqualTo(TypeList[i] == typeof(decimal) ? 38 : null));
                    Assert.That(schema[i].NumericScale, Is.EqualTo(TypeList[i] == typeof(decimal) ? 30 : null));
                });
            }
        }

        [Test]
        [Category("engine-v2")]
        public async Task ExecuteReaderAsyncWithDifferentDataTypes_ColumnSchemaAsync()
        {
            await using var connection = new FireboltConnection(UserConnectionString);
            await connection.OpenAsync();

            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = VariousTypeQuery;

            await using var reader = await command.ExecuteReaderAsync();
            Assert.That(await reader.ReadAsync(), Is.True);

            var schema = await reader.GetColumnSchemaAsync();
            Assert.That(schema, Has.Count.EqualTo(TypeList.Count));

            for (var i = 0; i < schema.Count; i++)
            {
                Assert.Multiple(() =>
                {
                    Assert.That(schema[i].ColumnName, Is.EqualTo(StructAliases[i]));
                    Assert.That(schema[i].ColumnOrdinal, Is.EqualTo(i));
                    Assert.That(schema[i].DataType, Is.EqualTo(TypeList[i]));
                    Assert.That(schema[i].ColumnSize, Is.EqualTo(-1));
                    Assert.That(schema[i].DataTypeName, Is.EqualTo(reader.GetDataTypeName(i)));
                    Assert.That(schema[i].AllowDBNull, Is.EqualTo(reader.IsDBNull(i)));
                    Assert.That(schema[i].NumericPrecision, Is.EqualTo(TypeList[i] == typeof(decimal) ? 38 : null));
                    Assert.That(schema[i].NumericScale, Is.EqualTo(TypeList[i] == typeof(decimal) ? 30 : null));
                });
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
            VerifyReturnedValues(reader);
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
            VerifyReturnedValues(reader);
        }

        [Test]
        [Category("engine-v2")]
        public void ExecuteReader_GetFieldValue_Generic_CorrectTypes()
        {
            using var connection = new FireboltConnection(UserConnectionString);
            connection.Open();
            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = VariousTypeQuery;

            using var reader = command.ExecuteReader();
            Assert.That(reader.Read(), Is.True);

            var getFieldValueGeneric = reader.GetType().GetMethod(nameof(FireboltDataReader.GetFieldValue))!;

            for (var i = 0; i < TypeList.Count; i++)
            {
                if (ExpectedValues[i] is null)
                {
                    Assert.That(reader.IsDBNull(i), Is.True, $"Column {i} should be NULL");
                    Assert.Throws<InvalidCastException>(() => reader.GetFieldValue<object>(i));
                }
                else
                {
                    var method = getFieldValueGeneric.MakeGenericMethod(TypeList[i]);
                    var value = method.Invoke(reader, new object[] { i });
                    Assert.That(value, Is.EqualTo(ExpectedValues[i]), $"Generic GetFieldValue mismatch at column {i}");
                }
            }
        }

        [Test]
        [Category("engine-v2")]
        public void ExecuteReader_GetFieldValue_Generic_WrongTypes()
        {
            using var connection = new FireboltConnection(UserConnectionString);
            connection.Open();
            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = VariousTypeQuery;

            using var reader = command.ExecuteReader();
            Assert.That(reader.Read(), Is.True);

            for (var i = 0; i < TypeList.Count; i++)
            {
                var ex = Assert.Throws<InvalidCastException>(() => reader.GetFieldValue<TimeSpan>(i));
                Assert.That(ex.Message,
                    ExpectedValues[i] is null
                        ? Does.Contain("is null (DBNull) and cannot be cast to TimeSpan")
                        : Does.Not.Contain("DBNull"));
            }
        }

        [Test]
        [Category("engine-v2")]
        public void CreateSelect_Struct_With_All_VariousTypes_StandardDictionary()
        {
            using var connection = new FireboltConnection(UserConnectionString);
            connection.Open();

            var select = (FireboltCommand)connection.CreateCommand();
            select.CommandText = $"SELECT s FROM {StructTable}";
            using var reader = select.ExecuteReader();
            Assert.That(reader.Read(), Is.True);

            var standardDic = (Dictionary<string, object?>)reader.GetValue(0);
            Assert.Multiple(() =>
            {
                Assert.That(standardDic, Is.Not.Null);
                Assert.That(standardDic.Keys, Has.Count.EqualTo(StructAliases.Count));
            });

            for (var i = 0; i < StructAliases.Count; i++)
            {
                var key = StructAliases[i];
                var expected = ExpectedValues[i];
                Assert.That(standardDic.ContainsKey(key), Is.True, $"Missing key '{key}' in struct");
                var actual = standardDic[key];
                if (expected is null)
                {
                    Assert.That(actual, Is.Null, $"Field '{key}' expected null");
                }
                else
                {
                    Assert.That(actual, Is.EqualTo(expected), $"Mismatch at '{key}'");
                }
            }
        }

        [Test]
        [Category("engine-v2")]
        public void CreateSelect_Struct_With_All_VariousTypes_GenericDictionary()
        {
            using var connection = new FireboltConnection(UserConnectionString);
            connection.Open();

            var select = (FireboltCommand)connection.CreateCommand();
            select.CommandText = $"SELECT s FROM {StructTable}";
            using var reader = select.ExecuteReader();
            Assert.That(reader.Read(), Is.True);

            var genericDic = reader.GetFieldValue<Dictionary<string, object?>>(0);
            Assert.That(genericDic.Keys, Has.Count.EqualTo(StructAliases.Count));

            for (var i = 0; i < StructAliases.Count; i++)
            {
                var key = StructAliases[i];
                var expected = ExpectedValues[i];
                Assert.That(genericDic.ContainsKey(key), Is.True, $"Missing key '{key}' in struct");
                var actual = genericDic[key];
                if (expected is null)
                {
                    Assert.That(actual, Is.Null, $"Field '{key}' expected null");
                }
                else
                {
                    Assert.That(actual, Is.EqualTo(expected), $"Mismatch at '{key}'");
                }
            }
        }

        [Test]
        [Category("engine-v2")]
        public void CreateSelect_Struct_With_All_VariousTypes_Poco()
        {
            using var connection = new FireboltConnection(UserConnectionString);
            connection.Open();

            var select = (FireboltCommand)connection.CreateCommand();
            select.CommandText = $"SELECT s FROM {StructTable}";
            using var reader = select.ExecuteReader();
            Assert.That(reader.Read(), Is.True);

            var poco = reader.GetFieldValue<VariousStructPoco>(0);
            var propByAlias = typeof(VariousStructPoco)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .ToDictionary(
                    f => f.GetCustomAttribute<FireboltStructNameAttribute>(true)!.Name,
                    f => f
                );

            for (var i = 0; i < StructAliases.Count; i++)
            {
                var alias = StructAliases[i];
                var expected = ExpectedValues[i];
                var value = propByAlias[alias].GetValue(poco);

                if (expected is null)
                {
                    Assert.That(value, Is.Null, $"POCO field '{alias}' expected null");
                }
                else
                {
                    Assert.That(value, Is.EqualTo(expected), $"POCO mismatch at '{alias}'");
                }
            }
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

        private class VariousStructPoco
        {
            // int group
            [FireboltStructName("col_int")] public int ColInt { get; init; }
            [FireboltStructName("col_int_null")] public int? ColIntNull { get; init; }
            [FireboltStructName("col_int_array")] public int[]? ColIntArray { get; init; }
            [FireboltStructName("col_int_null_array")] public int?[]? ColIntNullArray { get; init; }
            [FireboltStructName("col_int_array_null")] public int[]? ColIntArrayNull { get; init; }
            [FireboltStructName("col_int_array_null_array")] public int[][]? ColIntArrayNullArray { get; init; }
            [FireboltStructName("col_int_null_array_null_array")] public int?[][]? ColIntNullArrayNullArray { get; init; }
            [FireboltStructName("col_int_array_array_null")] public int[][]? ColIntArrayArrayNull { get; init; }

            // long group
            [FireboltStructName("col_long")] public long ColLong { get; init; }
            [FireboltStructName("col_long_null")] public long? ColLongNull { get; init; }
            [FireboltStructName("col_long_array")] public long[]? ColLongArray { get; init; }
            [FireboltStructName("col_long_null_array")] public long?[]? ColLongNullArray { get; init; }
            [FireboltStructName("col_long_array_null")] public long[]? ColLongArrayNull { get; init; }
            [FireboltStructName("col_long_array_null_array")] public long[][]? ColLongArrayNullArray { get; init; }
            [FireboltStructName("col_long_null_array_null_array")] public long?[][]? ColLongNullArrayNullArray { get; init; }
            [FireboltStructName("col_long_array_array_null")] public long[][]? ColLongArrayArrayNull { get; init; }

            // float group
            [FireboltStructName("col_float")] public float ColFloat { get; init; }
            [FireboltStructName("col_float_null")] public float? ColFloatNull { get; init; }
            [FireboltStructName("col_float_array")] public float[]? ColFloatArray { get; init; }
            [FireboltStructName("col_float_null_array")] public float?[]? ColFloatNullArray { get; init; }
            [FireboltStructName("col_float_array_null")] public float[]? ColFloatArrayNull { get; init; }
            [FireboltStructName("col_float_array_null_array")] public float[][]? ColFloatArrayNullArray { get; init; }
            [FireboltStructName("col_float_null_array_null_array")] public float?[][]? ColFloatNullArrayNullArray { get; init; }
            [FireboltStructName("col_float_array_array_null")] public float[][]? ColFloatArrayArrayNull { get; init; }

            // double group
            [FireboltStructName("col_double")] public double ColDouble { get; init; }
            [FireboltStructName("col_double_null")] public double? ColDoubleNull { get; init; }
            [FireboltStructName("col_double_array")] public double[]? ColDoubleArray { get; init; }
            [FireboltStructName("col_double_null_array")] public double?[]? ColDoubleNullArray { get; init; }
            [FireboltStructName("col_double_array_null")] public double[]? ColDoubleArrayNull { get; init; }
            [FireboltStructName("col_double_array_null_array")] public double[][]? ColDoubleArrayNullArray { get; init; }
            [FireboltStructName("col_double_null_array_null_array")] public double?[][]? ColDoubleNullArrayNullArray { get; init; }
            [FireboltStructName("col_double_array_array_null")] public double[][]? ColDoubleArrayArrayNull { get; init; }

            // text group
            [FireboltStructName("col_text")] public string? ColText { get; init; }
            [FireboltStructName("col_text_null")] public string? ColTextNull { get; init; }
            [FireboltStructName("col_text_array")] public string[]? ColTextArray { get; init; }
            [FireboltStructName("col_text_null_array")] public string?[]? ColTextNullArray { get; init; }
            [FireboltStructName("col_text_array_null")] public string[]? ColTextArrayNull { get; init; }
            [FireboltStructName("col_text_array_null_array")] public string[][]? ColTextArrayNullArray { get; init; }
            [FireboltStructName("col_text_null_array_null_array")] public string?[][]? ColTextNullArrayNullArray { get; init; }
            [FireboltStructName("col_text_array_array_null")] public string[][]? ColTextArrayArrayNull { get; init; }

            // date group
            [FireboltStructName("col_date")] public DateTime ColDate { get; init; }
            [FireboltStructName("col_date_null")] public DateTime? ColDateNull { get; init; }
            [FireboltStructName("col_date_array")] public DateTime[]? ColDateArray { get; init; }
            [FireboltStructName("col_date_null_array")] public DateTime?[]? ColDateNullArray { get; init; }
            [FireboltStructName("col_date_array_null")] public DateTime[]? ColDateArrayNull { get; init; }
            [FireboltStructName("col_date_array_null_array")] public DateTime?[][]? ColDateArrayNullArray { get; init; }
            [FireboltStructName("col_date_array_array")] public DateTime[][]? ColDateArrayArray { get; init; }
            [FireboltStructName("col_date_null_array_null_array")] public DateTime?[][]? ColDateNullArrayNullArray { get; init; }
            [FireboltStructName("col_date_array_array_null")] public DateTime[][]? ColDateArrayArrayNull { get; init; }

            // timestamp group
            [FireboltStructName("col_timestamp")] public DateTime ColTimestamp { get; init; }
            [FireboltStructName("col_timestamp_null")] public DateTime? ColTimestampNull { get; init; }
            [FireboltStructName("col_timestamp_array")] public DateTime[]? ColTimestampArray { get; init; }
            [FireboltStructName("col_timestamp_null_array")] public DateTime?[]? ColTimestampNullArray { get; init; }
            [FireboltStructName("col_timestamp_array_null")] public DateTime[]? ColTimestampArrayNull { get; init; }
            [FireboltStructName("col_timestamp_array_null_array")] public DateTime?[][]? ColTimestampArrayNullArray { get; init; }
            [FireboltStructName("col_timestamp_array_array")] public DateTime[][]? ColTimestampArrayArray { get; init; }
            [FireboltStructName("col_timestamp_null_array_null_array")] public DateTime?[][]? ColTimestampNullArrayNullArray { get; init; }
            [FireboltStructName("col_timestamp_array_array_null")] public DateTime[][]? ColTimestampArrayArrayNull { get; init; }

            // timestamptz group
            [FireboltStructName("col_timestamptz")] public DateTime ColTimestamptz { get; init; }
            [FireboltStructName("col_timestamptz_null")] public DateTime? ColTimestamptzNull { get; init; }
            [FireboltStructName("col_timestamptz_array")] public DateTime[]? ColTimestamptzArray { get; init; }
            [FireboltStructName("col_timestamptz_null_array")] public DateTime?[]? ColTimestamptzNullArray { get; init; }
            [FireboltStructName("col_timestamptz_array_null")] public DateTime[]? ColTimestamptzArrayNull { get; init; }
            [FireboltStructName("col_timestamptz_array_null_array")] public DateTime?[][]? ColTimestamptzArrayNullArray { get; init; }
            [FireboltStructName("col_timestamptz_array_array")] public DateTime[][]? ColTimestamptzArrayArray { get; init; }
            [FireboltStructName("col_timestamptz_null_array_null_array")] public DateTime?[][]? ColTimestamptzNullArrayNullArray { get; init; }
            [FireboltStructName("col_timestamptz_array_array_null")] public DateTime[][]? ColTimestamptzArrayArrayNull { get; init; }

            // boolean group
            [FireboltStructName("col_boolean")] public bool ColBoolean { get; init; }
            [FireboltStructName("col_boolean_null")] public bool? ColBooleanNull { get; init; }
            [FireboltStructName("col_boolean_array")] public bool[]? ColBooleanArray { get; init; }
            [FireboltStructName("col_boolean_null_array")] public bool?[]? ColBooleanNullArray { get; init; }
            [FireboltStructName("col_boolean_array_null")] public bool[]? ColBooleanArrayNull { get; init; }
            [FireboltStructName("col_boolean_array_null_array")] public bool[][]? ColBooleanArrayNullArray { get; init; }
            [FireboltStructName("col_boolean_null_array_null_array")] public bool?[][]? ColBooleanNullArrayNullArray { get; init; }
            [FireboltStructName("col_boolean_array_array_null")] public bool[][]? ColBooleanArrayArrayNull { get; init; }

            // decimal group
            [FireboltStructName("col_decimal")] public decimal ColDecimal { get; init; }
            [FireboltStructName("col_decimal_null")] public decimal? ColDecimalNull { get; init; }
            [FireboltStructName("col_decimal_array")] public decimal[]? ColDecimalArray { get; init; }
            [FireboltStructName("col_decimal_null_array")] public decimal?[]? ColDecimalNullArray { get; init; }
            [FireboltStructName("col_decimal_array_null")] public decimal[]? ColDecimalArrayNull { get; init; }
            [FireboltStructName("col_decimal_array_null_array")] public decimal?[][]? ColDecimalArrayNullArray { get; init; }
            [FireboltStructName("col_decimal_array_array")] public decimal[][]? ColDecimalArrayArray { get; init; }
            [FireboltStructName("col_decimal_null_array_null_array")] public decimal?[][]? ColDecimalNullArrayNullArray { get; init; }
            [FireboltStructName("col_decimal_array_array_null")] public decimal[][]? ColDecimalArrayArrayNull { get; init; }

            // bytea group
            [FireboltStructName("col_bytea")] public byte[]? ColBytea { get; init; }
            [FireboltStructName("col_bytea_null")] public byte[]? ColByteaNull { get; init; }

            // geography group
            [FireboltStructName("col_geography")] public string? ColGeography { get; init; }
            [FireboltStructName("col_geography_null")] public string? ColGeographyNull { get; init; }
            [FireboltStructName("col_geography_array")] public string[]? ColGeographyArray { get; init; }
            [FireboltStructName("col_geography_null_array")] public string?[]? ColGeographyNullArray { get; init; }
            [FireboltStructName("col_geography_array_null")] public string[]? ColGeographyArrayNull { get; init; }
            [FireboltStructName("col_geography_array_null_array")] public string[][]? ColGeographyArrayNullArray { get; init; }
            [FireboltStructName("col_geography_null_array_null_array")] public string?[][]? ColGeographyNullArrayNullArray { get; init; }
            [FireboltStructName("col_geography_array_array_null")] public string[][]? ColGeographyArrayArrayNull { get; init; }
        }
    }
}
