using System.Data.Common;
using System.Collections;
using System.Data;
using System.Text;
using System.Globalization;
using FireboltDotNetSdk.Utils;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;

namespace FireboltDotNetSdk.Tests
{
    public class FireboltDataReaderTest
    {
        [Test]
        public void Empty()
        {
            QueryResult result = new QueryResult
            {
                Rows = 0,
                Data = new()
            };

            DbDataReader reader = new FireboltDataReader(null, result);
            Assert.Multiple(() =>
            {
                Assert.That(reader.HasRows, Is.EqualTo(false));
                Assert.That(Assert.Throws<InvalidOperationException>(() => reader.GetValue(0))?.Message, Is.EqualTo("Read() must be called before fetching values"));
                Assert.That(reader.Read(), Is.EqualTo(false));

                Assert.That(reader.IsClosed, Is.EqualTo(false));
            });
            reader.Close();
            Assert.That(reader.IsClosed, Is.EqualTo(true));
        }

        [Test]
        public void EmptyEnumerator()
        {
            QueryResult result = new QueryResult
            {
                Rows = 0,
                Meta = new List<Meta>(),
                Data = new()
            };

            DbDataReader reader = new FireboltDataReader(null, result);
            IEnumerator enumerator = reader.GetEnumerator();
            Assert.That(enumerator.MoveNext(), Is.False);
            reader.Close();
        }

        [Test]
        public void Close()
        {
            DbDataReader reader = new FireboltDataReader(null, new QueryResult { Rows = 0, Data = new() });
            Assert.That(reader.IsClosed, Is.EqualTo(false));
            reader.Close();
            Assert.That(reader.IsClosed, Is.EqualTo(true));
        }

        [Test]
        public void CloseAsync()
        {
            DbDataReader reader = new FireboltDataReader(null, new QueryResult { Rows = 0, Data = new() });
            Assert.That(reader.IsClosed, Is.EqualTo(false));
            reader.CloseAsync().Wait(1);
            Assert.That(reader.IsClosed, Is.EqualTo(true));
        }

        [Test]
        public void OneRow()
        {
            QueryResult result = new QueryResult
            {
                Rows = 1,
                Meta = new List<Meta>() { new Meta() { Name = "number", Type = "int" }, new Meta() { Name = "name", Type = "text" } },
                Data = new List<List<object?>>() { new List<object?>() { 1, "one" } }
            };

            DbDataReader reader = new FireboltDataReader(null, result);
            Assert.Multiple(() =>
            {
                Assert.That(reader.HasRows, Is.EqualTo(true));
                Assert.That(reader.FieldCount, Is.EqualTo(2));
                Assert.That(reader.VisibleFieldCount, Is.EqualTo(2));
                Assert.That(reader.Depth, Is.EqualTo(0));
                Assert.That(reader.RecordsAffected, Is.EqualTo(0));

                Assert.That(reader.Read(), Is.EqualTo(true));
                Assert.That(reader.GetValue(0), Is.EqualTo(1));
                Assert.That(reader.GetValue("number"), Is.EqualTo(1));
                Assert.That(reader[0], Is.EqualTo(1));
                Assert.That(reader["number"], Is.EqualTo(1));

                Assert.That(reader.GetValue(1), Is.EqualTo("one"));
                Assert.That(reader[1], Is.EqualTo("one"));
                Assert.That(reader["name"], Is.EqualTo("one"));
            });

            Assert.Multiple(() =>
            {
                Assert.Throws<InvalidOperationException>(() => reader.GetValue(-1));
                Assert.Throws<InvalidOperationException>(() => reader.GetValue(2));
                Assert.That(reader.Read(), Is.EqualTo(false));
            });
        }

        [Test]
        public void OneRowEnumerator()
        {
            QueryResult result = new QueryResult
            {
                Rows = 1,
                Meta = new List<Meta>() { new Meta() { Name = "number", Type = "int" }, new Meta() { Name = "name", Type = "text" } },
                Data = new List<List<object?>>() { new List<object?>() { 1, "one" } }
            };

            DbDataReader reader = new FireboltDataReader(null, result);
            IEnumerator enumerator = reader.GetEnumerator();
            Assert.That(enumerator.MoveNext(), Is.True);
            object row = enumerator.Current;
            Assert.That(row, Is.Not.Null);
            DbDataRecord record = (DbDataRecord)row;
            Assert.Multiple(() =>
            {
                Assert.That(record.GetInt32(0), Is.EqualTo(1));
                Assert.That(record.GetString(1), Is.EqualTo("one"));
            });
            Assert.That(enumerator.MoveNext(), Is.False);
            reader.Close();
        }

        [Test]
        public void MultipleResults()
        {
            DbDataReader reader = new FireboltDataReader(null, new QueryResult());
            Assert.Multiple(() =>
            {
                Assert.That(reader.NextResult(), Is.EqualTo(false));
                Assert.That(reader.NextResultAsync().GetAwaiter().GetResult(), Is.EqualTo(false));
                Assert.That(reader.NextResultAsync(CancellationToken.None).GetAwaiter().GetResult(), Is.EqualTo(false));
                Assert.That(reader.NextResultAsync(new CancellationToken(false)).GetAwaiter().GetResult(), Is.EqualTo(false));
            });
            Assert.That(reader.NextResultAsync(new CancellationToken(true)), Is.Not.Null);
        }

        [Test]
        public void Numbers()
        {
            byte byteValue = 123;
            short shortValue = 1234;
            int intValue = 123456;
            long longValue = 1696773350000;
            float floatValue = 2.7f;
            double doubleValue = 3.1415926;
            double e = 2.71828;
            QueryResult result = new QueryResult
            {
                Rows = 1,
                Meta = new List<Meta>()
                {
                    new Meta() { Name = "b", Type = "int" },
                    new Meta() { Name = "s", Type = "int" },
                    new Meta() { Name = "i", Type = "int" }, // integer
                    new Meta() { Name = "l", Type = "long" }, // bigint
                    //new Meta() { Name = "dec", Type = "decimal" },
                    new Meta() { Name = "f", Type = "float" },
                    //new Meta() { Name = "r", Type = "real" },
                    new Meta() { Name = "d", Type = "double" },
                    new Meta() { Name = "flag", Type = "boolean" },
                    new Meta() { Name = "real_number", Type = "text" },
                    new Meta() { Name = "int_number", Type = "text" },
                    new Meta() { Name = "bool_string", Type = "text" },
                    new Meta() { Name = "num_bool_string", Type = "text" },
                },
                Data = new List<List<object?>>()
                {
                    new List<object?>() { byteValue, shortValue, intValue, longValue, floatValue, 3.1415926, true, "2.71828", "123", "true", "0" }
                }
            };

            DbDataReader reader = new FireboltDataReader(null, result);
            Assert.Multiple(() =>
            {
                Assert.That(reader.HasRows, Is.EqualTo(true));
                Assert.That(reader.Read(), Is.EqualTo(true));
            });
            Assert.Multiple(() =>
            {
                Assert.That(reader.GetByte(0), Is.EqualTo(byteValue));
                Assert.That(reader.GetInt16(0), Is.EqualTo(byteValue));
                Assert.That(reader.GetInt32(0), Is.EqualTo(byteValue));
                Assert.That(reader.GetInt64(0), Is.EqualTo(byteValue));
                Assert.That(reader.GetFloat(0), Is.EqualTo(byteValue));
                Assert.That(reader.GetDouble(0), Is.EqualTo(byteValue));
                Assert.That(reader.GetDecimal(0), Is.EqualTo(byteValue));
                Assert.That(reader.GetBoolean(0), Is.EqualTo(true));
                Assert.That(reader.GetChar(0), Is.EqualTo(Convert.ToChar(byteValue)));

                Assert.That(reader.GetByte(1), !Is.EqualTo(shortValue)); // shrunk
                Assert.That(reader.GetInt16(1), Is.EqualTo(shortValue));
                Assert.That(reader.GetInt32(1), Is.EqualTo(shortValue));
                Assert.That(reader.GetInt64(1), Is.EqualTo(shortValue));
                Assert.That(reader.GetFloat(1), Is.EqualTo(shortValue));
                Assert.That(reader.GetDouble(1), Is.EqualTo(shortValue));
                Assert.That(reader.GetDecimal(1), Is.EqualTo(shortValue));
                Assert.That(reader.GetBoolean(1), Is.EqualTo(true));
                Assert.That(reader.GetChar(1), Is.EqualTo(Convert.ToChar(shortValue)));


                Assert.That(reader.GetByte(2), !Is.EqualTo(intValue)); // shrunk
                Assert.That(reader.GetInt16(2), !Is.EqualTo(intValue)); // shrunk
                Assert.That(reader.GetInt32(2), Is.EqualTo(intValue));
                Assert.That(reader.GetInt64(2), Is.EqualTo(intValue));
                Assert.That(reader.GetFloat(2), Is.EqualTo(intValue));
                Assert.That(reader.GetDouble(2), Is.EqualTo(intValue));
                Assert.That(reader.GetDecimal(2), Is.EqualTo(intValue));
                Assert.That(reader.GetBoolean(2), Is.EqualTo(true));
                Assert.That(reader.GetChar(2), Is.EqualTo((char)intValue));

                Assert.That(reader.GetByte(3), !Is.EqualTo(longValue)); // shrunk
                Assert.That(reader.GetInt16(3), !Is.EqualTo(longValue)); // shrunk
                Assert.That(reader.GetInt32(3), !Is.EqualTo(longValue)); // shrunk
                Assert.That(reader.GetInt64(3), Is.EqualTo(longValue));
                Assert.That(reader.GetFloat(3), Is.EqualTo(longValue));
                Assert.That(reader.GetDouble(3), Is.EqualTo(longValue));
                Assert.That(reader.GetDecimal(3), Is.EqualTo(longValue));
                Assert.That(reader.GetBoolean(3), Is.EqualTo(true));
                Assert.That(reader.GetChar(3), Is.EqualTo((char)longValue));

                Assert.That(reader.GetByte(4), Is.EqualTo(2)); // shrunk
                Assert.That(reader.GetInt16(4), Is.EqualTo(2)); // shrunk
                Assert.That(reader.GetInt32(4), Is.EqualTo(2)); // shrunk
                Assert.That(reader.GetInt64(4), Is.EqualTo(2)); // shrunk
                Assert.That(reader.GetFloat(4), Is.EqualTo(floatValue));
                Assert.That(Math.Abs(reader.GetDouble(4) - floatValue), Is.LessThanOrEqualTo(0.001));
                Assert.That(Math.Abs((double)reader.GetDecimal(4) - floatValue), Is.LessThanOrEqualTo(0.001));
                Assert.That(reader.GetBoolean(4), Is.EqualTo(true));
                Assert.That(reader.GetChar(4), Is.EqualTo(Convert.ToChar((int)floatValue)));

                Assert.That(reader.GetByte(5), Is.EqualTo(3)); // shrunk
                Assert.That(reader.GetInt16(5), Is.EqualTo(3)); // shrunk
                Assert.That(reader.GetInt32(5), Is.EqualTo(3)); // shrunk
                Assert.That(reader.GetInt64(5), Is.EqualTo(3));  // shrunk
                Assert.That(reader.GetFloat(5), Is.EqualTo((float)doubleValue)); // shrunk
                Assert.That(reader.GetDouble(5), Is.EqualTo(doubleValue));
                Assert.That(reader.GetDecimal(5), Is.EqualTo(doubleValue));
                Assert.That(reader.GetBoolean(5), Is.EqualTo(true));
                Assert.That(reader.GetChar(5), Is.EqualTo(Convert.ToChar((int)doubleValue)));

                Assert.That(reader.GetBoolean(6), Is.EqualTo(true));

                Assert.That(Math.Abs(reader.GetFloat(7) - e), Is.LessThanOrEqualTo(0.001));
                Assert.That(reader.GetDouble(7), Is.EqualTo(e));
                Assert.That(reader.GetDecimal(7), Is.EqualTo(e));
                Assert.Throws<FormatException>(() => reader.GetBoolean(7));

                Assert.That(reader.GetByte(8), Is.EqualTo(byteValue));
                Assert.That(reader.GetInt16(8), Is.EqualTo(byteValue));
                Assert.That(reader.GetInt32(8), Is.EqualTo(byteValue));
                Assert.That(reader.GetInt64(8), Is.EqualTo(byteValue));
                Assert.Throws<FormatException>(() => reader.GetBoolean(8));

                Assert.That(reader.GetBoolean(9), Is.EqualTo(true));
                Assert.That(reader.GetChar(9), Is.EqualTo('t'));

                Assert.That(reader.GetBoolean(10), Is.EqualTo(false));
                Assert.That(reader.GetChar(10), Is.EqualTo('0'));
                Assert.That(reader.GetByte(10), Is.EqualTo(0));
                Assert.That(reader.GetInt16(10), Is.EqualTo(0));
                Assert.That(reader.GetInt32(10), Is.EqualTo(0));
                Assert.That(reader.GetInt64(10), Is.EqualTo(0));
            });
            Assert.Multiple(() =>
            {
                Assert.That(reader.Read(), Is.EqualTo(false));
                Assert.That(reader.IsClosed, Is.EqualTo(false));
            });
            reader.Close();
            Assert.That(reader.IsClosed, Is.EqualTo(true));
        }

        [Test]
        public void GetDateTime()
        {
            QueryResult result = new QueryResult
            {
                Rows = 1,
                Meta = new List<Meta>()
                {
                    new Meta() { Name = "b", Type = "TimestampTz" },
                },
                Data = new List<List<object?>>()
                {
                    new List<object?>() { "2022-05-10 21:01:02+00" }
                }
            };

            DbDataReader reader = new FireboltDataReader(null, result);
            Assert.Multiple(() =>
            {
                Assert.That(reader.Read(), Is.EqualTo(true));
                Assert.That(reader.GetDateTime(0), Is.EqualTo(DateTime.Parse("2022-05-10 21:01:02Z")));
            });
            Assert.That(reader.Read(), Is.EqualTo(false));
        }

        [Test]
        public void GetGuidAndString()
        {
            QueryResult result = new QueryResult
            {
                Rows = 1,
                Meta = new List<Meta>()
                {
                    new Meta() { Name = "guid", Type = "text" },
                    new Meta() { Name = "text", Type = "text" },
                    new Meta() { Name = "i", Type = "int" },
                },
                Data = new List<List<object?>>()
                {
                    new List<object?>() { "6B29FC40-CA47-1067-B31D-00DD010662DA", "not guid", 123 }
                }
            };

            DbDataReader reader = new FireboltDataReader(null, result);
            Assert.Multiple(() =>
            {
                Assert.That(reader.Read(), Is.EqualTo(true));
                Assert.That(reader.GetGuid(0), Is.EqualTo(new Guid("6B29FC40-CA47-1067-B31D-00DD010662DA")));
                Assert.Throws<FormatException>(() => reader.GetGuid(1));
                Assert.Throws<InvalidCastException>(() => reader.GetGuid(2));
                Assert.That(reader.GetString(0), Is.EqualTo("6B29FC40-CA47-1067-B31D-00DD010662DA"));
                Assert.That(reader.GetString(1), Is.EqualTo("not guid"));
                Assert.That(reader.GetString(2), Is.EqualTo("123"));
            });
            char[] allChars = new char[3];
            Assert.Multiple(() =>
            {
                Assert.That(reader.GetChars(2, 0, allChars, 0, 3), Is.EqualTo("123".ToCharArray().Length));
                Assert.That(allChars, Is.EqualTo("123".ToCharArray()));
            });
            char[] chars_0_2 = new char[2];
            Assert.Multiple(() =>
            {
                Assert.That(reader.GetChars(2, 0, chars_0_2, 0, 2), Is.EqualTo(2));
                Assert.That(chars_0_2, Is.EqualTo("12".ToCharArray()));
            });
            char[] chars_1_2 = new char[2];
            Assert.Multiple(() =>
            {
                Assert.That(reader.GetChars(2, 1, chars_1_2, 0, 2), Is.EqualTo(2));
                Assert.That(chars_1_2, Is.EqualTo("23".ToCharArray()));
            });
            char[] chars_1_1 = new char[1];
            Assert.Multiple(() =>
            {
                Assert.That(reader.GetChars(2, 1, chars_1_1, 0, 1), Is.EqualTo(1));
                Assert.That(chars_1_1, Is.EqualTo("2".ToCharArray()));
            });
            Stream stream = reader.GetStream(1);
            StreamReader streamReader = new StreamReader(stream, Encoding.UTF8);
            Assert.Multiple(() =>
            {
                Assert.That(streamReader.ReadToEnd(), Is.EqualTo("not guid"));
                Assert.That(reader.GetChars(2, 0, null, 0, 3), Is.EqualTo(0));
            });
            byte[] allBytes = new byte[8];
            Assert.Multiple(() =>
            {
                Assert.That(reader.GetBytes(1, 0, allBytes, 0, 8), Is.EqualTo(8));
                Assert.That(allBytes, Is.EqualTo(Encoding.UTF8.GetBytes("not guid")));
                Assert.That(reader.GetTextReader(1).ReadToEnd(), Is.EqualTo("not guid"));

            });
            Assert.That(reader.Read(), Is.EqualTo(false));
        }

        [Test]
        public void GetBytes()
        {
            string str = "hello";
            QueryResult result = new QueryResult
            {
                Rows = 1,
                Meta = new List<Meta>()
                {
                    new Meta() { Name = "b", Type = "bytea" },
                },
                Data = new List<List<object?>>()
                {
                    new List<object?>() { "\\x" + BitConverter.ToString(Encoding.UTF8.GetBytes(str)).Replace("-", "") }
                }
            };
            DbDataReader reader = new FireboltDataReader(null, result);
            Assert.That(reader.Read(), Is.EqualTo(true));

            byte[] allBytes = new byte[str.Length];
            Assert.Multiple(() =>
            {
                Assert.That(reader.GetBytes(0, 0, allBytes, 0, allBytes.Length), Is.EqualTo(allBytes.Length));
                Assert.That(allBytes, Is.EqualTo(Encoding.UTF8.GetBytes(str)));
            });
            Assert.That(reader.Read(), Is.EqualTo(false));
        }

        [Test]
        public void GetScalarMetadata()
        {
            QueryResult result = new QueryResult
            {
                Rows = 1,
                Meta = new List<Meta>()
                {
                    new Meta() { Name = "guid", Type = "text" },            // 0
                    new Meta() { Name = "text", Type = "text" },            // 1
                    new Meta() { Name = "i", Type = "int" },                // 2
                    new Meta() { Name = "n", Type = "numeric" },            // 3
                    new Meta() { Name = "n52", Type = "numeric(5,2)" },     // 4
                    new Meta() { Name = "dec", Type = "decimal(5,2)" },     // 5
                    new Meta() { Name = "bi", Type = "bigint" },            // 6
                    new Meta() { Name = "r", Type = "real" },               // 7
                    new Meta() { Name = "dp", Type = "DOUBLE PRECISION" },  // 8
                    new Meta() { Name = "d", Type = "DATE" },               // 9
                    new Meta() { Name = "ts", Type = "TIMESTAMP" },         //10
                    new Meta() { Name = "tstz", Type = "TIMESTAMPTZ" },     //11
                    new Meta() { Name = "b", Type = "boolean" },            //12
                    new Meta() { Name = "ba", Type = "bytea" },             //13
                },
                Data = new List<List<object?>>()
            };

            DbDataReader reader = new FireboltDataReader(null, result);
            Assert.Multiple(() =>
            {
                Assert.That(reader.GetName(0), Is.EqualTo("guid"));
                Assert.That(reader.GetName(1), Is.EqualTo("text"));
                Assert.That(reader.GetName(2), Is.EqualTo("i"));

                Assert.That(reader.GetOrdinal("guid"), Is.EqualTo(0));
                Assert.That(reader.GetOrdinal("text"), Is.EqualTo(1));
                Assert.That(reader.GetOrdinal("i"), Is.EqualTo(2));

                Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(string)));
                Assert.That(reader.GetFieldType(1), Is.EqualTo(typeof(string)));
                Assert.That(reader.GetFieldType(2), Is.EqualTo(typeof(int)));

                Assert.That(reader.GetFieldType(3), Is.EqualTo(typeof(decimal)));
                Assert.That(reader.GetFieldType(4), Is.EqualTo(typeof(decimal)));
                Assert.That(reader.GetFieldType(5), Is.EqualTo(typeof(decimal)));
                Assert.That(reader.GetFieldType(6), Is.EqualTo(typeof(long)));
                Assert.That(reader.GetFieldType(7), Is.EqualTo(typeof(float)));
                Assert.That(reader.GetFieldType(8), Is.EqualTo(typeof(double)));

                Assert.That(reader.GetFieldType(9), Is.EqualTo(typeof(DateTime)));
                Assert.That(reader.GetFieldType(10), Is.EqualTo(typeof(DateTime)));
                Assert.That(reader.GetFieldType(11), Is.EqualTo(typeof(DateTime)));

                Assert.That(reader.GetFieldType(12), Is.EqualTo(typeof(bool)));
                Assert.That(reader.GetFieldType(13), Is.EqualTo(typeof(byte[])));
            });
        }

        [TestCase("array(int)", typeof(int[]))]
        [TestCase("array(int null)", typeof(int?[]))]
        [TestCase("array(int null) null", typeof(int?[]))]
        [TestCase("array(int) null", typeof(int[]))]
        [TestCase("array(array(long null) null) null", typeof(long?[][]))]
        public void GetArrayMetadata(string typeDefinition, Type expectedType)
        {
            QueryResult result = new QueryResult
            {
                Rows = 1,
                Meta = new List<Meta>()
                {
                    new Meta() { Name = "column", Type = typeDefinition },
                },
                Data = new List<List<object?>>()
            };

            DbDataReader reader = new FireboltDataReader(null, result);
            Assert.That(reader.GetFieldType(0), Is.EqualTo(expectedType));
        }

        [TestCase("array(int")] // right parenthese is missing
        public void GetIncorrectArrayType(string typeDefinition)
        {
            QueryResult result = new QueryResult
            {
                Rows = 1,
                Meta = new List<Meta>()
                {
                    new Meta() { Name = "column", Type = typeDefinition },
                },
                Data = new List<List<object?>>()
            };

            DbDataReader reader = new FireboltDataReader(null, result);
            Assert.Throws<FireboltException>(() => reader.GetFieldType(0));
        }

        [Test]
        public void GetValues()
        {
            QueryResult result = new QueryResult
            {
                Rows = 1,
                Meta = new List<Meta>()
                {
                    new Meta() { Name = "guid", Type = "text" },
                    new Meta() { Name = "text", Type = "text" },
                    new Meta() { Name = "i", Type = "int" },
                },
                Data = new List<List<object?>>()
                {
                    new List<object?>() { "6B29FC40-CA47-1067-B31D-00DD010662DA", "not guid", 123 }
                }
            };

            DbDataReader reader = new FireboltDataReader(null, result);
            Assert.That(reader.Read(), Is.EqualTo(true));
            object[] values = new object[3];
            Assert.Multiple(() =>
            {
                Assert.That(reader.GetValues(values), Is.EqualTo(3));
                Assert.That(values, Is.EqualTo(new object[] { "6B29FC40-CA47-1067-B31D-00DD010662DA", "not guid", 123 }));
            });
            Assert.That(reader.Read(), Is.EqualTo(false));
        }

        [TestCase("int", 123)]
        [TestCase("long", 123456789)]
        [TestCase("float", 3.14)]
        [TestCase("double", 3.1415926)]
        [TestCase("numeric", 2.7)]
        [TestCase("decimal", 2.71828)]
        public void GetBytesUnsupportedType(string type, object value)
        {
            QueryResult result = new QueryResult
            {
                Rows = 1,
                Meta = new List<Meta>()
                {
                    new Meta() { Name = "x", Type = type },
                },
                Data = new List<List<object?>>()
                {
                    new List<object?>() { value }
                }
            };

            DbDataReader reader = new FireboltDataReader(null, result);
            Assert.Multiple(() =>
            {
                Assert.That(reader.Read(), Is.EqualTo(true));
                Assert.Throws<InvalidCastException>(() => reader.GetBytes(0, 0, Array.Empty<byte>(), 0, 0));
            });
            Assert.That(reader.Read(), Is.EqualTo(false));
        }

        [Test]
        public void NullValue()
        {
            QueryResult result = new QueryResult
            {
                Rows = 1,
                Meta = new List<Meta>()
                {
                    new Meta() { Name = "text", Type = "text" },
                },
                Data = new List<List<object?>>()
                {
                    new List<object?>() { null }
                }
            };

            DbDataReader reader = new FireboltDataReader(null, result);
            Assert.That(reader.Read(), Is.EqualTo(true));
            Assert.Multiple(async () =>
            {
                Assert.That(reader.GetValue(0), Is.EqualTo(DBNull.Value));
                Assert.That(reader.IsDBNull(0), Is.EqualTo(true));
                Assert.That(await reader.IsDBNullAsync(0), Is.EqualTo(true));

            });
            Assert.That(reader.Read(), Is.EqualTo(false));
        }

        [Test]
        public void GetUnsupportedType()
        {
            QueryResult result = new QueryResult
            {
                Rows = 1,
                Meta = new List<Meta>()
                {
                    new Meta() { Name = "t", Type = "TimestampTz" },
                    new Meta() { Name = "number", Type = "int" },
                },
                Data = new List<List<object?>>()
                {
                    new List<object?>() { "2022-05-10 21:01:02+00", 123 }
                }
            };

            DbDataReader reader = new FireboltDataReader(null, result);
            Assert.That(reader.Read(), Is.EqualTo(true));
            Assert.Throws<InvalidCastException>(() => reader.GetBoolean(0));
            Assert.Throws<InvalidCastException>(() => reader.GetByte(0));
            Assert.Throws<InvalidCastException>(() => reader.GetDecimal(0));
            Assert.Throws<InvalidCastException>(() => reader.GetDouble(0));
            Assert.Throws<InvalidCastException>(() => reader.GetFloat(0));
            Assert.Throws<InvalidCastException>(() => reader.GetGuid(0));
            Assert.Throws<InvalidCastException>(() => reader.GetInt16(0));
            Assert.Throws<InvalidCastException>(() => reader.GetInt32(0));
            Assert.Throws<InvalidCastException>(() => reader.GetInt64(0));
            Assert.Throws<InvalidCastException>(() => reader.GetChar(0));

            Assert.Throws<InvalidCastException>(() => reader.GetDateTime(1));
        }

        [TestCase(null)]
        [TestCase("my_table")]
        [TestCase("")]
        public async Task GetSchemaTable(string? tableName)
        {
            QueryResult result = new QueryResult
            {
                Rows = 0,
                Meta = new List<Meta>(),
                Data = new List<List<object?>>()
            };

            DbDataReader reader = new FireboltDataReader(tableName, result);

            DataTable? dataTable = reader.GetSchemaTable();
            Assert.That(dataTable?.TableName, Is.EqualTo(tableName ?? string.Empty));

            DataTable? dataTableAsync = await reader.GetSchemaTableAsync();
            Assert.That(dataTableAsync?.TableName, Is.EqualTo(tableName ?? string.Empty));
        }

        [Test]
        public void InfiniteNumberValue()
        {
            QueryResult result = new QueryResult
            {
                Rows = 1,
                Meta = new List<Meta>()
                {
                    new Meta() { Name = "inf", Type = "double" },
                    new Meta() { Name = "positive_inf", Type = "double" },
                    new Meta() { Name = "negative_inf", Type = "double" },
                },
                Data = new List<List<object?>>()
                {
                    new List<object?>() { "inf", "+inf", "-inf" }
                }
            };

            DbDataReader reader = new FireboltDataReader(null, result);
            Assert.Multiple(() =>
            {
                Assert.That(reader.Read(), Is.EqualTo(true));
                Assert.That(reader.GetFloat(0), Is.EqualTo(float.PositiveInfinity));
                Assert.That(reader.GetFloat(1), Is.EqualTo(float.PositiveInfinity));
                Assert.That(reader.GetFloat(2), Is.EqualTo(float.NegativeInfinity));

                Assert.That(reader.GetDouble(0), Is.EqualTo(double.PositiveInfinity));
                Assert.That(reader.GetDouble(1), Is.EqualTo(double.PositiveInfinity));
                Assert.That(reader.GetDouble(2), Is.EqualTo(double.NegativeInfinity));
            });

            // String representation of infinity may depnd on environment. It looks like ∞ on local environment and Infinity on github.
            string posInf = float.PositiveInfinity.ToString();
            string negInf = float.NegativeInfinity.ToString();
            Assert.Multiple(() =>
            {
                Assert.That(Assert.Throws<InvalidCastException>(() => reader.GetInt16(0))?.Message, Is.EqualTo($"Cannot convert {posInf} to Int16"));
                Assert.That(Assert.Throws<InvalidCastException>(() => reader.GetInt16(1))?.Message, Is.EqualTo($"Cannot convert {posInf} to Int16"));
                Assert.That(Assert.Throws<InvalidCastException>(() => reader.GetInt16(2))?.Message, Is.EqualTo($"Cannot convert {negInf} to Int16"));

                Assert.That(Assert.Throws<InvalidCastException>(() => reader.GetInt32(0))?.Message, Is.EqualTo($"Cannot convert {posInf} to Int32"));
                Assert.That(Assert.Throws<InvalidCastException>(() => reader.GetInt32(1))?.Message, Is.EqualTo($"Cannot convert {posInf} to Int32"));
                Assert.That(Assert.Throws<InvalidCastException>(() => reader.GetInt32(2))?.Message, Is.EqualTo($"Cannot convert {negInf} to Int32"));

                Assert.That(Assert.Throws<InvalidCastException>(() => reader.GetInt64(0))?.Message, Is.EqualTo($"Cannot convert {posInf} to Int64"));
                Assert.That(Assert.Throws<InvalidCastException>(() => reader.GetInt64(1))?.Message, Is.EqualTo($"Cannot convert {posInf} to Int64"));
                Assert.That(Assert.Throws<InvalidCastException>(() => reader.GetInt64(2))?.Message, Is.EqualTo($"Cannot convert {negInf} to Int64"));
            });

            Assert.That(reader.Read(), Is.EqualTo(false));
        }

        [Test]
        public void DataTableCompatibility()
        {
            QueryResult result = new QueryResult
            {
                Rows = 1,
                Meta = new List<Meta>()
                {
                    new Meta() { Name = "guid", Type = "text" },
                    new Meta() { Name = "text", Type = "text" },
                    new Meta() { Name = "i", Type = "int" },
                },
                Data = new List<List<object?>>()
                {
                    new List<object?>() { "6B29FC40-CA47-1067-B31D-00DD010662DA", "not guid", 123 }
                }
            };

            DbDataReader reader = new FireboltDataReader(null, result);
            DataTable dataTable = new DataTable();
            dataTable.Load(reader);

            Assert.Multiple(() =>
            {
                Assert.That(dataTable.Rows, Has.Count.EqualTo(1));
                Assert.That(dataTable.Rows[0]["guid"], Is.EqualTo("6B29FC40-CA47-1067-B31D-00DD010662DA"));
                Assert.That(dataTable.Rows[0]["text"], Is.EqualTo("not guid"));
                Assert.That(dataTable.Rows[0]["i"], Is.EqualTo(123));
            });
        }

        private static DbDataReader CreateStructReader()
        {
            var meta = new List<Meta>() { new Meta { Name = "plain", Type = "struct(int_val int, str_val text, arr_val array(decimal(5, 3)))" } };
            const string json = "{\"int_val\":42,\"str_val\":\"hello\",\"arr_val\":[1.23,4.56]}";
            var data = new List<List<object?>> { new List<object?> { json } };
            var result = new QueryResult { Rows = 1, Meta = meta, Data = data };

            DbDataReader reader = new FireboltDataReader(null, result);
            Assert.That(reader.Read(), Is.True);
            return reader;
        }

        [Test]
        public void GetFieldValueGeneric_StructHappyFlow()
        {
            var reader = CreateStructReader();

            // As dictionary
            var asDict = reader.GetFieldValue<Dictionary<string, object?>>(0);
            Assert.Multiple(() =>
            {
                Assert.That(asDict["int_val"], Is.EqualTo(42));
                Assert.That(asDict["str_val"], Is.EqualTo("hello"));
            });

            // Also verify POCO mapping using attribute names
            var poco = reader.GetFieldValue<TestPoco>(0);
            Assert.Multiple(() =>
            {
                Assert.That(poco.IntVal, Is.EqualTo(42));
                Assert.That(poco.StrVal, Is.EqualTo("hello"));
                Assert.That(poco.ArrVal!, Has.Length.EqualTo(2));
            });
        }

        [Test]
        public void GetFieldValueGeneric_Struct_UnhappyFlows()
        {
            var meta = new List<Meta>() { new Meta { Name = "plain", Type = "struct(int_val int, str_val text, arr_val array(decimal(5, 3)))" } };
            const string json = "{\"int_val\":42,\"str_val\":\"hello\",\"arr_val\":[1.23,4.56]}";
            var data = new List<List<object?>> { new List<object?> { json } };
            DbDataReader reader = new FireboltDataReader(null, new QueryResult { Rows = 1, Meta = meta, Data = data });
            Assert.That(reader.Read(), Is.True);

            // Not assignable cast
            Assert.Throws<InvalidCastException>(() => reader.GetFieldValue<int>(0));

            // Null column should throw
            var meta2 = new List<Meta>() { new Meta { Name = "plain", Type = "struct" } };
            var data2 = new List<List<object?>> { new List<object?> { null } };
            DbDataReader reader2 = new FireboltDataReader(null, new QueryResult { Rows = 1, Meta = meta2, Data = data2 });
            Assert.That(reader2.Read(), Is.True);
            Assert.Throws<InvalidCastException>(() => reader2.GetFieldValue<TestPoco>(0));
        }

        [Test]
        public void GetFieldValueGeneric_Struct_MixedAttributeAndImplicitNames()
        {
            var reader = CreateStructReader();

            var poco = reader.GetFieldValue<TestPocoMixed>(0);
            Assert.Multiple(() =>
            {
                Assert.That(poco.IntVal, Is.EqualTo(42));
                Assert.That(poco.StrVal, Is.EqualTo("hello"));
                Assert.That(poco.ArrVal!, Has.Length.EqualTo(2));
            });
        }

        [Test]
        public void GetFieldValueGeneric_Struct_PocoMissingFieldFromStruct()
        {
            var reader = CreateStructReader();

            var poco = reader.GetFieldValue<TestPocoMissing>(0);
            Assert.Multiple(() =>
            {
                Assert.That(poco.IntVal, Is.EqualTo(42));
                Assert.That(poco.StrVal, Is.EqualTo("hello"));
                // Missing field (arr_val) should be ignored without exceptions
            });
        }

        [Test]
        public void GetFieldValueGeneric_Struct_PocoDifferentShape_Throws()
        {
            var reader = CreateStructReader();

            var poco = reader.GetFieldValue<DifferentPocoWrongType>(0);
            Assert.Multiple(() =>
            {
                Assert.That(poco.TimestampVal, Is.EqualTo(default(DateTime)));
                Assert.That(poco.DoubleVal, Is.EqualTo(0.0));
            });
        }

        [Test]
        public void GetDouble_WithCzechCulture_NumericInDoubleColumn_Succeeds()
        {
            var originalCulture = CultureInfo.CurrentCulture;
            try
            {
                CultureInfo.CurrentCulture = new CultureInfo("cs-CZ");
                var result = new QueryResult
                {
                    Rows = 1,
                    Meta = new List<Meta>() { new Meta() { Name = "val", Type = "double" } },
                    Data = new List<List<object?>>() { new List<object?>() { 2.71828 } }
                };

                DbDataReader reader = new FireboltDataReader(null, result);
                Assert.Multiple(() =>
                {
                    Assert.That(reader.Read(), Is.EqualTo(true));
                    Assert.That(Math.Abs(reader.GetDouble(0) - 2.71828), Is.LessThanOrEqualTo(0.000001));
                    Assert.That(reader.GetString(0), Is.EqualTo("2,71828"));
                });
            }
            finally
            {
                CultureInfo.CurrentCulture = originalCulture;
            }
        }

        [Test]
        public void GetDecimal_WithCzechCulture_Succeeds()
        {
            var originalCulture = CultureInfo.CurrentCulture;
            try
            {
                CultureInfo.CurrentCulture = new CultureInfo("cs-CZ");
                var result = new QueryResult
                {
                    Rows = 1,
                    Meta = new List<Meta>() { new Meta() { Name = "val", Type = "Decimal(18, 5)" } },
                    Data = new List<List<object?>>() { new List<object?>() { "2.71828" } }
                };

                DbDataReader reader = new FireboltDataReader(null, result);
                Assert.Multiple(() =>
                {
                    Assert.That(reader.Read(), Is.EqualTo(true));
                    Assert.That(Math.Abs(reader.GetDouble(0) - 2.71828), Is.LessThanOrEqualTo(0.000001));
                    Assert.That(reader.GetString(0), Is.EqualTo("2,71828"));
                });
            }
            finally
            {
                CultureInfo.CurrentCulture = originalCulture;
            }
        }

        class TestPoco
        {
            [FireboltStructName("int_val")] public int IntVal { get; init; }
            [FireboltStructName("str_val")] public string StrVal { get; init; } = string.Empty;
            [FireboltStructName("arr_val")] public float[]? ArrVal { get; init; }
        }

        class TestPocoMixed
        {
            // Attribute for one field
            [FireboltStructName("int_val")] public int IntVal { get; init; }

            // Implicit snake_case mapping (no attribute)
            public string StrVal { get; init; } = string.Empty;

            // Implicit snake_case mapping (no attribute)
            public float[]? ArrVal { get; init; }
        }

        class TestPocoMissing
        {
            [FireboltStructName("int_val")] public int IntVal { get; init; }
            // No attribute, mapped by snake_case to str_val
            public string StrVal { get; init; } = string.Empty;
            // Deliberately missing ArrVal property
        }

        class DifferentPocoWrongType
        {
            public DateTime TimestampVal { get; init; }
            [FireboltStructName("double_val")] public double DoubleVal { get; init; }
        }

        [Test]
        public async Task SchemaTable_And_ColumnSchema_Are_Populated_From_Meta()
        {
            var resultTypes = new List<Type>
            {
                typeof(int),
                typeof(decimal),
                typeof(string),
            };
            var metas = new List<Meta>
            {
                new Meta { Name = "i", Type = "int" },
                new Meta { Name = "dec", Type = "decimal(8, 3) null" },
                new Meta { Name = "t", Type = "text" },
            };
            var result = new QueryResult { Rows = 0, Meta = metas, Data = new List<List<object?>>() };
            var reader = new FireboltDataReader(null, result);

            var schema = reader.GetSchemaTable();
            Assert.That(schema, Is.Not.Null);
            Assert.That(schema!.Rows, Has.Count.EqualTo(metas.Count));

            // Validate each row
            for (var i = 0; i < metas.Count; i++)
            {
                var row = schema.Rows[i];
                var expectedType = TypesConverter.GetType(ColumnType.Of(TypesConverter.GetFullColumnTypeName(metas[i])));
                Assert.Multiple(() =>
                {
                    Assert.That(row["ColumnName"], Is.EqualTo(metas[i].Name));
                    Assert.That(row["ColumnOrdinal"], Is.EqualTo(i));
                    Assert.That(row["DataTypeName"], Is.EqualTo(metas[i].Type));
                    Assert.That(row["ColumnSize"], Is.EqualTo(-1));
                    Assert.That(row["DataType"], Is.EqualTo(expectedType));
                });

                if (metas[i].Type.StartsWith("decimal"))
                {
                    Assert.Multiple(() =>
                    {
                        Assert.That(row["NumericPrecision"], Is.Not.EqualTo(DBNull.Value));
                        Assert.That(row["NumericScale"], Is.Not.EqualTo(DBNull.Value));
                        Assert.That(Convert.ToInt32(row["NumericPrecision"]), Is.EqualTo(8));
                        Assert.That(Convert.ToInt32(row["NumericScale"]), Is.EqualTo(3));
                        Assert.That(row["AllowDBNull"], Is.EqualTo(true));
                    });
                }
                else
                {
                    Assert.Multiple(() =>
                    {
                        Assert.That(row["NumericPrecision"], Is.EqualTo(DBNull.Value));
                        Assert.That(row["NumericScale"], Is.EqualTo(DBNull.Value));
                        Assert.That(row["AllowDBNull"], Is.EqualTo(false));
                    });
                }
            }

            // Validate column schema APIs are available and consistent
            var columnSchema = reader.GetColumnSchema();
            var columnSchemaAsync = await reader.GetColumnSchemaAsync();
            Assert.Multiple(() =>
            {
                Assert.That(columnSchema, Is.Not.Null);
                Assert.That(columnSchema, Has.Count.EqualTo(metas.Count));
                Assert.That(columnSchemaAsync, Is.Not.Null);
                Assert.That(columnSchemaAsync, Has.Count.EqualTo(metas.Count));
            });
            for (int i = 0; i < metas.Count; i++)
            {
                Assert.Multiple(() =>
                {
                    Assert.That(columnSchema[i].ColumnName, Is.EqualTo(metas[i].Name));
                    Assert.That(columnSchema[i].ColumnOrdinal, Is.EqualTo(i));
                    Assert.That(columnSchema[i].DataTypeName, Is.EqualTo(metas[i].Type));
                    Assert.That(columnSchema[i].DataType, Is.EqualTo(resultTypes[i]));
                });
            }
        }
    }
}
