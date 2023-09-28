using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using static FireboltDotNetSdk.Client.FireResponse;
using FireboltDotNetSdk.Utils;
using System.Data.Common;
using System.Data;
using Moq;

namespace FireboltDotNetSdk.Tests
{
    public class MockClient : FireboltClient
    {
        private static Mock<HttpClient> httpClientMock = new();
        private readonly string? _response;
        public string? Query { get; private set; }

        public MockClient(string? response) : base("id", "secret", "", null, httpClientMock.Object)
        {
            _response = response;
            TokenSecureStorage.CacheToken(new LoginResponse("token", "60", "type"), "id", "secret").Wait();
            EstablishConnection().Wait();
        }

        override public Task<string?> ExecuteQuery(string? engineEndpoint, string? databaseName, string? accountId, HashSet<string> setParamList, string query)
        {
            Query = query;
            return Task.FromResult<string?>(_response);
        }

        override public Task<GetAccountIdByNameResponse> GetAccountIdByNameAsync(string account, CancellationToken cancellationToken)
        {
            return Task.FromResult<GetAccountIdByNameResponse>(new GetAccountIdByNameResponse());
        }
    }

    [TestFixture]
    public class FireboltCommandTest
    {
        private const string mockConnectionString = $"database=db;clientid=id;clientsecret=secret;endpoint=ep;account=a;env=mock";

        [TestCase("SET param=1")]
        [TestCase("SET param=1,param=2")]
        public void SetTest(string commandText)
        {
            var connection = new FireboltConnection(mockConnectionString) { Client = new MockClient(""), EngineUrl = "engine" };
            var cs = new FireboltCommand(connection, commandText, new FireboltParameterCollection());
            Assert.IsEmpty(cs.SetParamList);
            cs.ExecuteNonQuery();
            Assert.IsNotEmpty(cs.SetParamList);
        }

        [TestCase("Select 1")]
        public void ExecuteSelectWhenConnectionIsMissingTest(string commandText)
        {
            var cs = new FireboltCommand { CommandText = commandText };
            FireboltException? exception = Assert.Throws<FireboltException>(() => cs.ExecuteReader());
            Assert.NotNull(exception);
            Assert.That(exception!.Message, Is.EqualTo("Unable to execute SQL as no connection was initialised. Create command using working connection"));
        }

        [Test]
        public void GetOriginalJsonDataExceptionTest()
        {
            var cs = createCommand("select 1", null);
            DbDataReader reader = cs.ExecuteReader();
            Assert.False(reader.Read());
        }

        [Test]
        public void ExecuteRederNoQuery()
        {
            var cs = createCommand(null, null);
            Assert.That(Assert.Throws<InvalidOperationException>(() => cs.ExecuteReader()).Message, Is.EqualTo("Command is undefined"));
        }


        [Test]
        public void GetOriginalJsonDataTest()
        {
            string response =
            "{\"query\":{\"query_id\": \"16FDB86662938757\"},\"meta\":[{\"name\": \"uint8\",\"type\": \"int\"}, {\"name\": \"dinf\",\"type\": \"double\"}],\"data\":[[1, \"inf\"]],\"rows\": 1,\"statistics\":{\"elapsed\": 0.000620069,\"rows_read\": 1,\"bytes_read\": 1,\"time_before_execution\": 0.000409657,\"time_to_execute\": 0.000208377,\"scanned_bytes_cache\": 0,\"scanned_bytes_storage\": 0}}";

            var cs = createCommand("select 1", response);
            DbDataReader reader = cs.ExecuteReader();
            Assert.True(reader.Read());
            Assert.That(reader.GetInt16(0), Is.EqualTo(1));
            Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(int)));
            Assert.That(reader.GetDouble(1), Is.EqualTo(double.PositiveInfinity));
            Assert.That(reader.GetFieldType(1), Is.EqualTo(typeof(double)));
            Assert.False(reader.Read());
        }

        [TestCase("SET param=1")]
        [TestCase("SET param=1,param=2")]
        public void ClearSetListTest(string commandText)
        {
            var connection = new FireboltConnection(mockConnectionString) { Client = new MockClient(""), EngineUrl = "engine" };
            var cs = new FireboltCommand(connection, commandText, new FireboltParameterCollection());

            cs.ExecuteNonQuery();
            Assert.IsNotEmpty(cs.SetParamList);
            Assert.IsNotEmpty(connection.SetParamList);
            cs.ClearSetList();
            Assert.IsEmpty(cs.SetParamList);
            Assert.IsEmpty(connection.SetParamList);
        }

        [Test]
        public void ExistParamListTest()
        {
            var cs = new FireboltCommand();
            cs.Parameters.Add(new FireboltParameter("@param", "abcd"));
            Assert.IsTrue(cs.Parameters.Any());
        }

        [TestCase("hello")]
        [TestCase(123)]
        [TestCase(true)]
        [TestCase(false)]
        public void CreateParameterTest(object value)
        {
            var cs = new FireboltCommand();
            DbParameter parameter = cs.CreateParameter();
            parameter.ParameterName = "@param";
            parameter.Value = value;
            cs.Parameters.Add(parameter);
            Assert.IsTrue(cs.Parameters.Any());
        }

        [Test]
        public void DateTimeParameterTest()
        {
            CreateParameterTest(DateTime.UnixEpoch);
        }

        [Test]
        public void ListParameterTest()
        {
            Assert.Throws<KeyNotFoundException>(() => CreateParameterTest(new List())); // List parameter is not supported
        }

        [TestCase("abcd", "'abcd'")]
        [TestCase("test' OR '1' == '1", "'test\\' OR \\'1\\' == \\'1'")]
        [TestCase("test\\", "'test\\\\'")]
        [TestCase("some\0value", "'some\\\\0value'")]
        public void SetParamListStrTest(string commandText, string expect)
        {
            SetParamTest(commandText, expect);
        }

        [TestCase(1, "1")]
        public void SetParamListIntTest(int commandText, string expect)
        {
            SetParamTest(commandText, expect);
        }

        [TestCase(15000000000L, "15000000000")]
        public void SetParamListLongTest(long commandText, string expect)
        {
            SetParamTest(commandText, expect);
        }

        [TestCase(1.123, "1.123")]
        public void SetParamListDoubleTest(double commandText, string expect)
        {
            SetParamTest(commandText, expect);
        }

        [TestCase(1.123, "1.123")]
        public void SetParamListDecimalTest(decimal commandText, string expect)
        {
            SetParamTest(commandText, expect);
        }

        [TestCase(5.75F, "5.75")]
        public void SetParamListFloatTest(float commandText, string expect)
        {
            SetParamTest(commandText, expect);
        }

        [TestCase(null, "NULL")]
        public void SetParamListNullTest(string commandText, string expect)
        {
            SetParamTest(commandText, expect);
        }

        [Test]
        public void SetParamListDatesTest()
        {
            SetParamTest(DateTime.Now, "'" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "'");
        }

        private void SetParamTest(object commandText, object expect)
        {
            MockClient client = new MockClient("");
            var cs = new FireboltCommand(new FireboltConnection(mockConnectionString) { Client = client, EngineUrl = "engine" }, "@param", new FireboltParameterCollection());
            cs.Parameters.Add(new FireboltParameter("@param", commandText));
            cs.ExecuteNonQuery();
            Assert.That(client.Query, Is.EqualTo(expect));
        }

        [Test]
        public void TimestampTzTest()
        {
            var responseWithTimestampTz =
                "{\"query\":{\"query_id\": \"1739956EA85D7645\"},\"meta\":[{\"name\": \"CAST('2022-05-10 23:01:02.12345 Europe\\/Berlin' AS timestamptz)\",\"type\": \"TimestampTz\"}],\"data\":[[\"2022-05-10 21:01:02.12345+00\"]],\"rows\": 1,\"statistics\":{\"elapsed\": 0.001312549,\"rows_read\": 1,\"bytes_read\": 1,\"time_before_execution\": 0.000553908,\"time_to_execute\": 0.000173431,\"scanned_bytes_cache\": 0,\"scanned_bytes_storage\": 0}}";
            NewMeta newMeta = ResponseUtilities.getFirstRow(responseWithTimestampTz);
            DateTime expectedTimestampTz = DateTime.Parse("2022-05-10 21:01:02Z");
            expectedTimestampTz = expectedTimestampTz.AddTicks(1234500);

            Assert.That(newMeta.Data[0], Is.EqualTo(expectedTimestampTz));
            Assert.That(newMeta.Meta, Is.EqualTo("TimestampTz"));
        }

        [Test]
        public void TimestampTzWithoutMicrosecondsTest()
        {
            var responseWithTimestampTz =
                "{\"query\":{\"query_id\": \"1739956EA85D7645\"},\"meta\":[{\"name\": \"CAST('2022-05-10 23:01:02.0 Europe\\/Berlin' AS timestamptz)\",\"type\": \"TimestampTz\"}],\"data\":[[\"2022-05-10 21:01:02.0+00\"]],\"rows\": 1,\"statistics\":{\"elapsed\": 0.001312549,\"rows_read\": 1,\"bytes_read\": 1,\"time_before_execution\": 0.000553908,\"time_to_execute\": 0.000173431,\"scanned_bytes_cache\": 0,\"scanned_bytes_storage\": 0}}";
            NewMeta newMeta = ResponseUtilities.getFirstRow(responseWithTimestampTz);
            DateTime expectedTimestampTz = DateTime.Parse("2022-05-10 21:01:02Z");

            Assert.That(newMeta.Data[0], Is.EqualTo(expectedTimestampTz));
            Assert.That(newMeta.Meta, Is.EqualTo("TimestampTz"));
        }

        [Test]
        public void TimestampTzWithoutMicrosecondsWithSecondsInTzTest()
        {
            var responseWithTimestampTz =
                "{\"query\":{\"query_id\": \"173ACEC4A4AD8DDD\"},\"meta\":[{\"name\": \"CAST('1111-01-05 17:04:42' AS timestamptz)\",\"type\": \"TimestampTz\"}],\"data\":[[\"1111-01-05 17:04:42+05:53:28\"]],\"rows\": 1,\"statistics\":{\"elapsed\": 0.001197308,\"rows_read\": 1,\"bytes_read\": 1,\"time_before_execution\": 0.000535819,\"time_to_execute\": 0.000163099,\"scanned_bytes_cache\": 0,\"scanned_bytes_storage\": 0}}";
            NewMeta newMeta = ResponseUtilities.getFirstRow(responseWithTimestampTz);
            DateTime expectedTimestampTz = DateTime.Parse("1111-01-05 11:11:14Z");

            Assert.That(newMeta.Data[0], Is.EqualTo(expectedTimestampTz));
            Assert.That(newMeta.Meta, Is.EqualTo("TimestampTz"));
        }

        [Test]
        public void TimestampTzWithNotAllMicrosecondsWithSecondsInTzTest()
        {
            var responseWithTimestampTz =
                "{\"query\":{\"query_id\": \"173ACEC4A4AD8DDD\"},\"meta\":[{\"name\": \"CAST('1111-01-05 17:04:42' AS timestamptz)\",\"type\": \"TimestampTz\"}],\"data\":[[\"1111-01-05 17:04:42.123+05:53:28\"]],\"rows\": 1,\"statistics\":{\"elapsed\": 0.001197308,\"rows_read\": 1,\"bytes_read\": 1,\"time_before_execution\": 0.000535819,\"time_to_execute\": 0.000163099,\"scanned_bytes_cache\": 0,\"scanned_bytes_storage\": 0}}";
            NewMeta newMeta = ResponseUtilities.getFirstRow(responseWithTimestampTz);
            DateTime expectedTimestampTz = DateTime.Parse("1111-01-05 11:11:14.123Z");

            Assert.That(newMeta.Data[0], Is.EqualTo(expectedTimestampTz));
            Assert.That(newMeta.Meta, Is.EqualTo("TimestampTz"));
        }

        [Test]
        public void TimestampTzWithNonUnixTimestamp()
        {
            var responseWithTimestampTz =
                "{\"query\":{\"query_id\": \"173ACEC4A4AD8DA0\"},\"meta\":[{\"name\": \"CAST('1111-01-05 17:04:42.123456' AS timestamptz)\",\"type\": \"TimestampTz\"}],\"data\":[[\"1111-01-05 17:04:42.123456+05:53:28\"]],\"rows\": 1,\"statistics\":{\"elapsed\": 0.001270414,\"rows_read\": 1,\"bytes_read\": 1,\"time_before_execution\": 0.000541517,\"time_to_execute\": 0.000200035,\"scanned_bytes_cache\": 0,\"scanned_bytes_storage\": 0}}";
            NewMeta newMeta = ResponseUtilities.getFirstRow(responseWithTimestampTz);
            DateTime expectedDateTime = DateTime.Parse("1111-01-05 11:11:14.123456Z");
            Assert.That(newMeta.Data[0], Is.EqualTo(expectedDateTime));
            Assert.That(newMeta.Meta, Is.EqualTo("TimestampTz"));
        }

        [Test]
        public void TimestampNtzTest()
        {
            var responseWithTimestampNtz =
                "{\"query\":{\"query_id\": \"1739956EA85D7646\"},\"meta\":[{\"name\": \"CAST('2022-05-10 23:01:02.123456' AS timestampntz)\",\"type\": \"TimestampNtz\"}],\"data\":[[\"2022-05-10 23:01:02.123456\"]],\"rows\": 1,\"statistics\":{\"elapsed\": 0.001318462,\"rows_read\": 1,\"bytes_read\": 1,\"time_before_execution\": 0.000547007,\"time_to_execute\": 0.000249659,\"scanned_bytes_cache\": 0,\"scanned_bytes_storage\": 0}}";
            NewMeta newMeta = ResponseUtilities.getFirstRow(responseWithTimestampNtz);
            DateTime expectedTimestampNtz = new DateTime(2022, 5, 10, 23, 1, 2, 0);
            expectedTimestampNtz = expectedTimestampNtz.AddTicks(1234560);
            Assert.That(newMeta.Data[0], Is.EqualTo(expectedTimestampNtz));
            Assert.That(newMeta.Meta, Is.EqualTo("TimestampNtz"));
        }

        [Test]
        public void PgDateTest()
        {
            var responseWithPgDate =
                "{\"query\":{\"query_id\": \"1739956EA85D7647\"},\"meta\":[{\"name\": \"CAST('2022-05-10' AS pgdate)\",\"type\": \"PGDate\"}],\"data\":[[\"2022-05-10\"]],\"rows\": 1,\"statistics\":{\"elapsed\": 0.001887076,\"rows_read\": 1,\"bytes_read\": 1,\"time_before_execution\": 0.000528582,\"time_to_execute\": 0.000203717,\"scanned_bytes_cache\": 0,\"scanned_bytes_storage\": 0}}";
            NewMeta newMeta = ResponseUtilities.getFirstRow(responseWithPgDate);
            DateOnly expectedDate = DateOnly.FromDateTime(new DateTime(2022, 5, 10, 23, 1, 2, 0));
            Assert.That(newMeta.Data[0], Is.EqualTo(expectedDate));
            Assert.That(newMeta.Meta, Is.EqualTo("Date"));
        }

        [Test]
        public void TimestampNtzWithNonUnixTimestamp()
        {
            var responseWithTimestampNtz =
                "{\"query\":{\"query_id\": \"1739956EA85D7646\"},\"meta\":[{\"name\": \"CAST('0001-05-10 23:01:02.123' AS timestampntz)\",\"type\": \"TimestampNtz\"}],\"data\":[[\"0001-05-10 23:01:02.123\"]],\"rows\": 1,\"statistics\":{\"elapsed\": 0.001318462,\"rows_read\": 1,\"bytes_read\": 1,\"time_before_execution\": 0.000547007,\"time_to_execute\": 0.000249659,\"scanned_bytes_cache\": 0,\"scanned_bytes_storage\": 0}}";
            NewMeta newMeta = ResponseUtilities.getFirstRow(responseWithTimestampNtz);
            DateTime expectedTimestampNtz = new DateTime(1, 5, 10, 23, 1, 2, 123);
            Assert.That(newMeta.Data[0], Is.EqualTo(expectedTimestampNtz));
            Assert.That(newMeta.Meta, Is.EqualTo("TimestampNtz"));
        }
        [Test]
        public void NullNothingTest()
        {
            var responseWithTimestampNtz =
                "{\"query\":{\"query_id\": \"173B162657CAA798\"},\"meta\":[{\"name\": \"NULL\",\"type\": \"Nullable(Nothing)\"}],\"data\":[[null]],\"rows\": 1,\"statistics\":{\"elapsed\": 0.001364262,\"rows_read\": 1,\"bytes_read\": 1,\"time_before_execution\": 0.001206089,\"time_to_execute\": 0.000155675,\"scanned_bytes_cache\": 0,\"scanned_bytes_storage\": 0}}";
            NewMeta newMeta = ResponseUtilities.getFirstRow(responseWithTimestampNtz);
            Assert.That(newMeta.Data[0], Is.EqualTo(null));
            Assert.That(newMeta.Meta, Is.EqualTo("Null"));
        }

        [Test]
        public void NullTimestampTzTest()
        {
            var responseWithTimestampNtz =
                "{\"query\":{\"query_id\": \"173B1C44D6EB9C1F\"},\"meta\":[{\"name\": \"CAST(NULL AS timestamptz)\",\"type\": \"Nullable(TimestampTz)\"}],\"data\":[[null]],\"rows\": 1,\"statistics\":{\"elapsed\": 0.001241091,\"rows_read\": 1,\"bytes_read\": 1,\"time_before_execution\": 0.000519624,\"time_to_execute\": 0.000231926,\"scanned_bytes_cache\": 0,\"scanned_bytes_storage\": 0}}";
            NewMeta newMeta = ResponseUtilities.getFirstRow(responseWithTimestampNtz);
            Assert.That(newMeta.Data[0], Is.EqualTo(null));
            Assert.That(newMeta.Meta, Is.EqualTo("TimestampTz"));
        }

        [Test]
        public void NullIntTest()
        {
            var responseWithTimestampNtz =
                "{\"query\":{\"query_id\": \"173B1C44D6EB9C32\"},\"meta\":[{\"name\": \"CAST(NULL AS int)\",\"type\": \"int null\"}],\"data\":[[null]],\"rows\": 1,\"statistics\":{\"elapsed\": 0.001383618,\"rows_read\": 1,\"bytes_read\": 1,\"time_before_execution\": 0.001199649,\"time_to_execute\": 0.000181567,\"scanned_bytes_cache\": 0,\"scanned_bytes_storage\": 0}}";

            NewMeta newMeta = ResponseUtilities.getFirstRow(responseWithTimestampNtz);
            Assert.That(newMeta.Data[0], Is.EqualTo(null));
            Assert.That(newMeta.Meta, Is.EqualTo("Int"));
        }

        [Test]
        public void NullStringTest()
        {
            var responseWithTimestampNtz =
                "{\"query\":{\"query_id\": \"173B1C44D6EB9C33\"},\"meta\":[{\"name\": \"CAST(NULL AS text)\",\"type\": \"string null\"}],\"data\":[[null]],\"rows\": 1,\"statistics\":{\"elapsed\": 0.001355843,\"rows_read\": 1,\"bytes_read\": 1,\"time_before_execution\": 0.001176689,\"time_to_execute\": 0.000176776,\"scanned_bytes_cache\": 0,\"scanned_bytes_storage\": 0}}";

            NewMeta newMeta = ResponseUtilities.getFirstRow(responseWithTimestampNtz);
            Assert.That(newMeta.Data[0], Is.EqualTo(null));
            Assert.That(newMeta.Meta, Is.EqualTo("String"));
        }

        [TestCase("true", true)]
        [TestCase("false", false)]
        public void BooleanTest(string data, bool expect)
        {
            var responseWithPgDate =
                "{\"query\":{\"query_id\": \"1739956EA85D7647\"},\"meta\":[{\"name\": \"CAST(1 AS boolean)\",\"type\": \"boolean\"}],\"data\":[[\"" +
                data +
                "\"]],\"rows\": 1,\"statistics\":{\"elapsed\": 0.001887076,\"rows_read\": 1,\"bytes_read\": 1,\"time_before_execution\": 0.000528582,\"time_to_execute\": 0.000203717,\"scanned_bytes_cache\": 0,\"scanned_bytes_storage\": 0}}";
            NewMeta newMeta = ResponseUtilities.getFirstRow(responseWithPgDate);
            Assert.That(newMeta.Data[0], Is.EqualTo(expect));
            Assert.That(newMeta.Meta, Is.EqualTo("Boolean"));
        }

        [Test]
        public void NullBooleanTest()
        {
            var responseWithPgDate =
                "{\"query\":{\"query_id\": \"1739956EA85D7647\"},\"meta\":[{\"name\": \"CAST(1 AS boolean)\",\"type\": \"boolean null\"}],\"data\":[[null]],\"rows\": 1,\"statistics\":{\"elapsed\": 0.001887076,\"rows_read\": 1,\"bytes_read\": 1,\"time_before_execution\": 0.000528582,\"time_to_execute\": 0.000203717,\"scanned_bytes_cache\": 0,\"scanned_bytes_storage\": 0}}";
            NewMeta newMeta = ResponseUtilities.getFirstRow(responseWithPgDate);
            Assert.That(newMeta.Data[0], Is.EqualTo(null));
            Assert.That(newMeta.Meta, Is.EqualTo("Boolean"));
        }

        [TestCase(CommandType.StoredProcedure)]
        [TestCase(CommandType.TableDirect)]
        public void WrongCommandType(CommandType commandType)
        {
            DbCommand command = new FireboltCommand();
            Assert.Throws<NotSupportedException>(() => command.CommandType = commandType);
        }

        [Test]
        public void TextCommandType()
        {
            DbCommand command = new FireboltCommand();
            Assert.That(command.CommandType, Is.EqualTo(CommandType.Text));
            command.CommandType = CommandType.Text;
            Assert.That(command.CommandType, Is.EqualTo(CommandType.Text));
        }

        [Test]
        public void SetConnection()
        {
            DbCommand command = new FireboltCommand();
            Assert.That(command.Connection, Is.EqualTo(null));
            var connection = new FireboltConnection(mockConnectionString) { Client = new MockClient(""), EngineUrl = "engine" };
            command.Connection = connection;
            Assert.That(command.Connection, Is.EqualTo(connection));
        }

        [Test]
        public void SetTransaction()
        {
            DbCommand command = new FireboltCommand();
            Assert.That(command.Transaction, Is.EqualTo(null));
            command.Transaction = null;
            Assert.That(command.Transaction, Is.EqualTo(null));
            Mock<DbTransaction> transaction = new();
            Assert.Throws<NotSupportedException>(() => command.Transaction = transaction.Object);
        }

        [Test]
        public void CommandTimeout()
        {
            DbCommand command = new FireboltCommand();
            Assert.Throws<NotImplementedException>(() => { int x = command.CommandTimeout; });
            Assert.Throws<NotImplementedException>(() => command.CommandTimeout = 123);
        }

        [TestCase(UpdateRowSource.None)]
        [TestCase(UpdateRowSource.OutputParameters)]
        [TestCase(UpdateRowSource.FirstReturnedRecord)]
        [TestCase(UpdateRowSource.Both)]
        public void SetUpdatedRowSource(UpdateRowSource updateRowSource)
        {
            DbCommand command = new FireboltCommand();
            Assert.Throws<NotImplementedException>(() => { var x = command.UpdatedRowSource; });
            Assert.Throws<NotImplementedException>(() => command.UpdatedRowSource = updateRowSource);
        }

        [TestCase(true)]
        [TestCase(false)]
        public void SetDesignTimeVisible(bool visible)
        {
            DbCommand command = new FireboltCommand();
            Assert.Throws<NotImplementedException>(() => { var x = command.DesignTimeVisible; });
            Assert.Throws<NotImplementedException>(() => command.DesignTimeVisible = visible);
        }

        [Test]
        public void Cancel()
        {
            DbCommand command = new FireboltCommand();
            Assert.Throws<NotImplementedException>(() => command.Cancel());
        }

        [Test]
        public void ExecuteScalar()
        {
            DbCommand command = new FireboltCommand();
            Assert.Throws<NotImplementedException>(() => command.ExecuteScalar());
        }

        [Test]
        public void Prepare()
        {
            DbCommand command = new FireboltCommand();
            Assert.Throws<NotImplementedException>(() => command.Prepare());
        }

        private DbCommand createCommand(string? query, string? response)
        {
            return new FireboltCommand(new FireboltConnection(mockConnectionString) { Client = new MockClient(response), EngineUrl = "engine" }, query, new FireboltParameterCollection());
        }
    }
}