using System.Data.Common;
using System.Collections;
using System.Data;
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
            Assert.That(reader.HasRows, Is.EqualTo(false));
            Assert.Throws<ArgumentOutOfRangeException>(() => reader.GetValue(0));
            Assert.That(reader.Read(), Is.EqualTo(false));

            Assert.That(reader.IsClosed, Is.EqualTo(false));
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
            Assert.False(enumerator.MoveNext());
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
            Assert.That(reader[1], Is.EqualTo("one"));
            Assert.That(reader["name"], Is.EqualTo("one"));

            Assert.Throws<IndexOutOfRangeException>(() => reader.GetValue(-1));
            Assert.Throws<IndexOutOfRangeException>(() => reader.GetValue(2));
            Assert.That(reader.Read(), Is.EqualTo(false));
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
            Assert.True(enumerator.MoveNext());
            object row = enumerator.Current;
            Assert.NotNull(row);
            DbDataRecord record = (DbDataRecord)row;
            Assert.That(record.GetInt32(0), Is.EqualTo(1));
            Assert.That(record.GetString(1), Is.EqualTo("one"));
            Assert.False(enumerator.MoveNext());
            reader.Close();
        }

        [Test]
        public void MultipleResults()
        {
            DbDataReader reader = new FireboltDataReader(null, new QueryResult());
            Assert.Throws<FireboltException>(() => reader.NextResult());
            Assert.Throws<FireboltException>(() => reader.NextResultAsync());
            Assert.Throws<FireboltException>(() => reader.NextResultAsync(CancellationToken.None));
            Assert.Throws<FireboltException>(() => reader.NextResultAsync(new CancellationToken(true)));
            Assert.Throws<FireboltException>(() => reader.NextResultAsync(new CancellationToken(false)));
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
                },
                Data = new List<List<object?>>()
                {
                    new List<object?>() { byteValue, shortValue, intValue, longValue, floatValue, 3.1415926, true, "2.71828", "123" }
                }
            };

            DbDataReader reader = new FireboltDataReader(null, result);
            Assert.That(reader.HasRows, Is.EqualTo(true));
            Assert.That(reader.Read(), Is.EqualTo(true));

            Assert.That(reader.GetByte(0), Is.EqualTo(byteValue));
            Assert.That(reader.GetInt16(0), Is.EqualTo(byteValue));
            Assert.That(reader.GetInt32(0), Is.EqualTo(byteValue));
            Assert.That(reader.GetInt64(0), Is.EqualTo(byteValue));
            Assert.That(reader.GetFloat(0), Is.EqualTo(byteValue));
            Assert.That(reader.GetDouble(0), Is.EqualTo(byteValue));
            Assert.That(reader.GetDecimal(0), Is.EqualTo(byteValue));
            Assert.That(reader.GetBoolean(0), Is.EqualTo(true));

            Assert.That(reader.GetByte(1), !Is.EqualTo(shortValue)); // shrunk
            Assert.That(reader.GetInt16(1), Is.EqualTo(shortValue));
            Assert.That(reader.GetInt32(1), Is.EqualTo(shortValue));
            Assert.That(reader.GetInt64(1), Is.EqualTo(shortValue));
            Assert.That(reader.GetFloat(1), Is.EqualTo(shortValue));
            Assert.That(reader.GetDouble(1), Is.EqualTo(shortValue));
            Assert.That(reader.GetDecimal(1), Is.EqualTo(shortValue));
            Assert.That(reader.GetBoolean(1), Is.EqualTo(true));

            Assert.That(reader.GetByte(2), !Is.EqualTo(intValue)); // shrunk
            Assert.That(reader.GetInt16(2), !Is.EqualTo(intValue)); // shrunk
            Assert.That(reader.GetInt32(2), Is.EqualTo(intValue));
            Assert.That(reader.GetInt64(2), Is.EqualTo(intValue));
            Assert.That(reader.GetFloat(2), Is.EqualTo(intValue));
            Assert.That(reader.GetDouble(2), Is.EqualTo(intValue));
            Assert.That(reader.GetDecimal(2), Is.EqualTo(intValue));
            Assert.That(reader.GetBoolean(2), Is.EqualTo(true));

            Assert.That(reader.GetByte(3), !Is.EqualTo(longValue)); // shrunk
            Assert.That(reader.GetInt16(3), !Is.EqualTo(longValue)); // shrunk
            Assert.That(reader.GetInt32(3), !Is.EqualTo(longValue)); // shrunk
            Assert.That(reader.GetInt64(3), Is.EqualTo(longValue));
            Assert.That(reader.GetFloat(3), Is.EqualTo(longValue));
            Assert.That(reader.GetDouble(3), Is.EqualTo(longValue));
            Assert.That(reader.GetDecimal(3), Is.EqualTo(longValue));
            Assert.That(reader.GetBoolean(3), Is.EqualTo(true));

            Assert.That(reader.GetByte(4), Is.EqualTo(2)); // shrunk
            Assert.That(reader.GetInt16(4), Is.EqualTo(2)); // shrunk
            Assert.That(reader.GetInt32(4), Is.EqualTo(2)); // shrunk
            Assert.That(reader.GetInt64(4), Is.EqualTo(2)); // shrunk
            Assert.That(reader.GetFloat(4), Is.EqualTo(floatValue));
            Assert.True(Math.Abs(reader.GetDouble(4) - floatValue) <= 0.001);
            Assert.True(Math.Abs((double)reader.GetDecimal(4) - floatValue) <= 0.001);
            Assert.That(reader.GetBoolean(4), Is.EqualTo(true));

            Assert.That(reader.GetByte(5), Is.EqualTo(3)); // shrunk
            Assert.That(reader.GetInt16(5), Is.EqualTo(3)); // shrunk
            Assert.That(reader.GetInt32(5), Is.EqualTo(3)); // shrunk
            Assert.That(reader.GetInt64(5), Is.EqualTo(3));  // shrunk
            Assert.That(reader.GetFloat(5), Is.EqualTo((float)doubleValue)); // shrunk
            Assert.That(reader.GetDouble(5), Is.EqualTo(doubleValue));
            Assert.That(reader.GetDecimal(5), Is.EqualTo(doubleValue));
            Assert.That(reader.GetBoolean(5), Is.EqualTo(true));

            Assert.That(reader.GetBoolean(6), Is.EqualTo(true));

            Assert.True(Math.Abs((double)reader.GetFloat(7) - e) <= 0.001);
            Assert.That(reader.GetDouble(7), Is.EqualTo(e));
            Assert.That(reader.GetDecimal(7), Is.EqualTo(e));

            Assert.That(reader.GetByte(8), Is.EqualTo(byteValue));
            Assert.That(reader.GetInt16(8), Is.EqualTo(byteValue));
            Assert.That(reader.GetInt32(8), Is.EqualTo(byteValue));
            Assert.That(reader.GetInt64(8), Is.EqualTo(byteValue));

            Assert.That(reader.Read(), Is.EqualTo(false));

            Assert.That(reader.IsClosed, Is.EqualTo(false));
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
            Assert.That(reader.Read(), Is.EqualTo(true));
            Assert.That(reader.GetDateTime(0), Is.EqualTo(DateTime.Parse("2022-05-10 21:01:02Z")));
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
            Assert.That(reader.Read(), Is.EqualTo(true));

            Assert.That(reader.GetGuid(0), Is.EqualTo(new Guid("6B29FC40-CA47-1067-B31D-00DD010662DA")));
            Assert.Throws<FormatException>(() => reader.GetGuid(1));
            Assert.Throws<InvalidCastException>(() => reader.GetGuid(2));

            Assert.That(reader.GetString(0), Is.EqualTo("6B29FC40-CA47-1067-B31D-00DD010662DA"));
            Assert.That(reader.GetString(1), Is.EqualTo("not guid"));
            Assert.That(reader.GetString(2), Is.EqualTo("123"));

            Assert.That(reader.Read(), Is.EqualTo(false));
        }


        [Test]
        public void GetMetadata()
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
            };

            DbDataReader reader = new FireboltDataReader(null, result);
            Assert.That(reader.GetName(0), Is.EqualTo("guid"));
            Assert.That(reader.GetName(1), Is.EqualTo("text"));
            Assert.That(reader.GetName(2), Is.EqualTo("i"));

            Assert.That(reader.GetOrdinal("guid"), Is.EqualTo(0));
            Assert.That(reader.GetOrdinal("text"), Is.EqualTo(1));
            Assert.That(reader.GetOrdinal("i"), Is.EqualTo(2));
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
            Assert.That(reader.GetValues(values), Is.EqualTo(3));
            Assert.That(values, Is.EqualTo(new object[] { "6B29FC40-CA47-1067-B31D-00DD010662DA", "not guid", 123 }));
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
            Assert.That(reader.GetValue(0), Is.EqualTo(DBNull.Value));
            Assert.That(reader.IsDBNull(0), Is.EqualTo(true));
            Assert.That(reader.Read(), Is.EqualTo(false));
        }
    }
}
