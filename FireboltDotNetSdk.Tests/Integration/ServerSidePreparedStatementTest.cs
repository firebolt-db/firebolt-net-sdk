using System.Data.Common;
using FireboltDotNetSdk.Client;
using FireboltNETSDK.Exception;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    internal class ServerSidePreparedStatementTest : IntegrationTest
    {
        private static string USER_CONNECTION_STRING = ConnectionString(new Tuple<string, string?>("preparedStatementParamStyle", "FbNumeric"));

        [Test]
        [Category("engine-v2")]
        public void ExecutePreparedStatement()
        {
            using var connection = new FireboltConnection(USER_CONNECTION_STRING);
            connection.Open();

            FireboltCommand command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "SELECT $1, $2";
            command.Parameters.Add(CreateParameter(command, "$1", 1));
            command.Parameters.Add(CreateParameter(command, "$2", 2));

            // Execute the query asynchronously using the synchronous method
            using (DbDataReader reader = command.ExecuteReader())
            {
                Assert.Multiple(() =>
                {
                    Assert.That(reader.Read(), Is.EqualTo(true));
                    Assert.That(reader.GetInt64(0), Is.EqualTo(1));
                    Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(long)));
                    Assert.That(reader.GetInt64(1), Is.EqualTo(2));
                    Assert.That(reader.GetFieldType(1), Is.EqualTo(typeof(long)));
                });
                Assert.That(reader.Read(), Is.EqualTo(false));
            }
        }
        [Test]
        [Category("engine-v2")]
        public void ExecutePreparedStatementNative()
        {
            using var connection = new FireboltConnection(ConnectionString());
            connection.Open();

            FireboltCommand command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "SELECT @1,@2";
            command.Parameters.Add(CreateParameter(command, "@1", 1));
            command.Parameters.Add(CreateParameter(command, "@2", 2));

            // Execute the query asynchronously using the synchronous method
            using (DbDataReader reader = command.ExecuteReader())
            {
                Assert.Multiple(() =>
                {
                    Assert.That(reader.Read(), Is.EqualTo(true));
                    Assert.That(reader.GetInt32(0), Is.EqualTo(1));
                    Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(int)));
                    Assert.That(reader.GetInt32(1), Is.EqualTo(2));
                    Assert.That(reader.GetFieldType(1), Is.EqualTo(typeof(int)));
                });
                Assert.That(reader.Read(), Is.EqualTo(false));
            }
        }
        
        [Test]
        [Category("engine-v2")]
        public void ExecutePreparedStatementWithVariousDataTypes()
        {
            using var connection = new FireboltConnection(USER_CONNECTION_STRING);
            connection.Open();
            var now = DateTime.UtcNow;

            FireboltCommand command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "SELECT $1::int, $2::long, $3::decimal(38,4), $4::real, $5::double, $6::array(int), $7::datetime, $8::text";
            command.Parameters.Add(CreateParameter(command, "$1", 42));
            command.Parameters.Add(CreateParameter(command, "$2", 9007199254740991));
            command.Parameters.Add(CreateParameter(command, "$3", 12345.6789m));
            command.Parameters.Add(CreateParameter(command, "$4", 3.14f));
            command.Parameters.Add(CreateParameter(command, "$5", 2.718281828459045));
            command.Parameters.Add(CreateParameter(command, "$6", new[] {1, 2, 3}));
            command.Parameters.Add(CreateParameter(command, "$7", now));
            command.Parameters.Add(CreateParameter(command, "$8", "Hello Firebolt!"));

            // Execute the query asynchronously using the synchronous method
            using (DbDataReader reader = command.ExecuteReader())
            {
                Assert.Multiple(() =>
                {
                    Assert.That(reader.Read(), Is.EqualTo(true));
                    Assert.That(reader.GetInt32(0), Is.EqualTo(42));
                    Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(int)));
                    Assert.That(reader.GetInt64(1), Is.EqualTo(9007199254740991));
                    Assert.That(reader.GetFieldType(1), Is.EqualTo(typeof(long)));
                    Assert.That(reader.GetDecimal(2), Is.EqualTo(12345.6789m));
                    Assert.That(reader.GetFieldType(2), Is.EqualTo(typeof(decimal)));
                    Assert.That(reader.GetFloat(3), Is.EqualTo(3.14f));
                    Assert.That(reader.GetFieldType(3), Is.EqualTo(typeof(float)));
                    Assert.That(reader.GetDouble(4), Is.EqualTo(2.718281828459045));
                    Assert.That(reader.GetFieldType(4), Is.EqualTo(typeof(double)));
                    Assert.That(reader.GetFieldValue<int?[]>(5), Is.EqualTo(new[] {1, 2, 3}));
                    Assert.That(reader.GetFieldType(5), Is.EqualTo(typeof(int?[])));
                    Assert.That(reader.GetDateTime(6), Is.EqualTo(now));
                    Assert.That(reader.GetFieldType(6), Is.EqualTo(typeof(DateTime)));
                    Assert.That(reader.GetString(7), Is.EqualTo("Hello Firebolt!"));
                    Assert.That(reader.GetFieldType(7), Is.EqualTo(typeof(string)));
                });
                Assert.That(reader.Read(), Is.EqualTo(false));
            }
        }
        
        [Test]
        [Category("engine-v2")]
        public void ExecutePreparedStatementWithMoreParametersProvided()
        {
            using var connection = new FireboltConnection(USER_CONNECTION_STRING);
            connection.Open();

            FireboltCommand command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "SELECT $1, $2";
            command.Parameters.Add(CreateParameter(command, "$1", 1));
            command.Parameters.Add(CreateParameter(command, "$2", 2));
            command.Parameters.Add(CreateParameter(command, "$3", 3));

            // Execute the query asynchronously using the synchronous method
            using (DbDataReader reader = command.ExecuteReader())
            {
                Assert.Multiple(() =>
                {
                    Assert.That(reader.Read(), Is.EqualTo(true));
                    Assert.That(reader.GetInt64(0), Is.EqualTo(1));
                    Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(long)));
                    Assert.That(reader.GetInt64(1), Is.EqualTo(2));
                    Assert.That(reader.GetFieldType(1), Is.EqualTo(typeof(long)));
                });
                Assert.That(reader.Read(), Is.EqualTo(false));
            }
        }
        
        [Test]
        [Category("engine-v2")]
        public void ExecutePreparedStatementWithRandomParametersIndexProvided()
        {
            using var connection = new FireboltConnection(USER_CONNECTION_STRING);
            connection.Open();

            FireboltCommand command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "SELECT $34, $72";
            command.Parameters.Add(CreateParameter(command, "$34", 1));
            command.Parameters.Add(CreateParameter(command, "$72", 2));

            // Execute the query asynchronously using the synchronous method
            using (DbDataReader reader = command.ExecuteReader())
            {
                Assert.Multiple(() =>
                {
                    Assert.That(reader.Read(), Is.EqualTo(true));
                    Assert.That(reader.GetInt64(0), Is.EqualTo(1));
                    Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(long)));
                    Assert.That(reader.GetInt64(1), Is.EqualTo(2));
                    Assert.That(reader.GetFieldType(1), Is.EqualTo(typeof(long)));
                });
                Assert.That(reader.Read(), Is.EqualTo(false));
            }
        }
        
        [Test]
        [Category("engine-v2")]
        public void FailWhenNotEnoughParametersProvided()
        {
            using var connection = new FireboltConnection(USER_CONNECTION_STRING);
            connection.Open();

            FireboltCommand command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "SELECT $1, $2";
            command.Parameters.Add(CreateParameter(command, "$1", 1));

            // Execute the query asynchronously using the synchronous method
            var fireboltException = Assert.Throws<FireboltStructuredException>(() => command.ExecuteReader());
            Assert.That(fireboltException.Message, Does.Contain("Line 1, Column 12: Query referenced positional parameter $2, but it was not set"));
        }
        
        [Test]
        [Category("engine-v2")]
        public void FailWhenIncorrectParameterProvided()
        {
            using var connection = new FireboltConnection(USER_CONNECTION_STRING);
            connection.Open();

            FireboltCommand command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = "SELECT $1, $2";
            command.Parameters.Add(CreateParameter(command, "$1", 1));
            command.Parameters.Add(CreateParameter(command, "$3", 1));

            // Execute the query asynchronously using the synchronous method
            var fireboltException = Assert.Throws<FireboltStructuredException>(() => command.ExecuteReader());
            Assert.That(fireboltException.Message, Does.Contain("Line 1, Column 12: Query referenced positional parameter $2, but it was not set"));
        }
    }
}
