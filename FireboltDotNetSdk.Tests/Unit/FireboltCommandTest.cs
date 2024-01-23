using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using static FireboltDotNetSdk.Client.FireResponse;
using FireboltDotNetSdk.Utils;
using System.Data.Common;
using System.Data;
using Moq;
using FireboltDoNetSdk.Utils;
using System.Runtime.CompilerServices;

namespace FireboltDotNetSdk.Tests
{
    public class MockClient : FireboltClient
    {
        private static Mock<HttpClient> httpClientMock = new();
        const string connectionString = "database=testdb.ib;clientid=testuser;clientsecret=test_pwd;account=accountname;endpoint=api.mock.firebolt.io";
        private static FireboltConnection connection = new FireboltConnection(connectionString);


        private readonly string? _response;
        public string? Query { get; private set; }

        public MockClient(string? response) : base(connection, "id", "secret", "", null, null, httpClientMock.Object)
        {
            _response = response;
            TokenSecureStorage.CacheToken(new LoginResponse("token", "60", "type"), "id", "secret").Wait();
            EstablishConnection().Wait();
        }

        override public Task<string?> ExecuteQuery(string? engineEndpoint, string? databaseName, string? accountId, HashSet<string> setParamList, string query)
        {
            Query = query;
            return Task.FromResult(_response);
        }

        public override async Task<string?> ExecuteQueryAsync(string? engineEndpoint, string? databaseName, string? accountId,
                         string query, HashSet<string> setParamList, CancellationToken cancellationToken)
        {
            Query = query;
            return await Task.FromResult(_response);
        }

        override public Task<GetAccountIdByNameResponse> GetAccountIdByNameAsync(string account, CancellationToken cancellationToken)
        {
            return Task.FromResult(new GetAccountIdByNameResponse());
        }

        override public Task<ConnectionResponse> ConnectAsync(string? engineName, string database, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        protected override Task<LoginResponse> Login(string id, string secret, string env)
        {
            throw new NotImplementedException();
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
            Assert.That(cs.SetParamList, Is.EqualTo(new HashSet<string>(new string[] { commandText.Replace("SET ", "") })));
        }

        [TestCase("SET param=1")]
        [TestCase("SET param=1,param=2")]
        public async Task SetTestAsync(string commandText)
        {
            var connection = new FireboltConnection(mockConnectionString) { Client = new MockClient(""), EngineUrl = "engine" };
            var cs = new FireboltCommand(connection, commandText, new FireboltParameterCollection());
            Assert.IsEmpty(cs.SetParamList);
            await cs.ExecuteNonQueryAsync();
            Assert.That(cs.SetParamList, Is.EqualTo(new HashSet<string>(new string[] { commandText.Replace("SET ", "") })));
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
        public void ExecuteReaderNullResponseTest()
        {
            var cs = createCommand("select 1", null);
            DbDataReader reader = cs.ExecuteReader();
            Assert.False(reader.Read());
        }

        [TestCase(null, null, "SQL command is null")]
        [TestCase(null, "{}", "SQL command is null")]
        [TestCase("select 1", "null", "No result produced")] // produces null QueryResult
        public void ExecuteRederInvalidQuery(string? query, string? response, string expectedErrorMessage)
        {
            var cs = createCommand(query, response);
            Assert.That(Assert.Throws<InvalidOperationException>(() => cs.ExecuteReader())?.Message, Is.EqualTo(expectedErrorMessage));
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
        [TestCase(new byte[0])]
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
        public void DateOnlyParameterTest()
        {
            CreateParameterTest(DateOnly.Parse("2023-10-24"));
        }

        [Test]
        public void ListParameterTest()
        {
            CreateParameterTest(new List<string>());
        }

        [Test]
        public void SetParamListTest()
        {
            SetParamTest(new List<string>(), "[]");
            SetParamTest(new List<object?>(), "[]");
            SetParamTest(new List<object?>() { "a", 1, null }, "['a',1,NULL]");
            SetParamTest(new int[0], "[]");
            SetParamTest(new long[] { 1, 2, 3 }, "[1,2,3]");
            SetParamTest(new double[][] { new double[] { 2.718282, 3.1415926 }, new double[] { 6.022, 1.6 } }, "[[2.718282,3.1415926],[6.022,1.6]]");
        }

        [TestCase("2023-10-24 16:21:42", "yyyy-MM-dd HH:mm:ss")]
        [TestCase("2023-10-24 16:21:42.00", "yyyy-MM-dd HH:mm:ss")]
        [TestCase("2023-10-24 00:00:00.00", "yyyy-MM-dd")]
        [TestCase("2023-10-24", "yyyy-MM-dd")]
        public void SetParamDateTimeTest(string date, string format)
        {
            DateTime dateTime = DateTime.Parse(date);
            SetParamTest(dateTime, "'" + dateTime.ToString(format) + "'");
        }

        [TestCase("2023-10-24 16:21:42+02")]
        public void SetParamDateTimeOffsetTest(string date)
        {
            DateTimeOffset dateTime = DateTimeOffset.Parse(date);
            SetParamTest(dateTime, "'" + dateTime.ToString("yyyy-MM-dd HH:mm:ss.FFFFFFz") + "'");
        }

        [TestCase("2023-10-24")]
        public void SetParamDateOnlyTest(string date)
        {
            DateOnly dateTime = DateOnly.Parse(date);
            SetParamTest(dateTime, "'" + dateTime.ToString("yyyy-MM-dd") + "'");
        }

        [Test]
        public void SetParamDateTest()
        {
            DateTime dateTime = DateTime.Now;
            SetParamTest(dateTime, "'" + dateTime.ToString("yyyy-MM-dd HH:mm:ss") + "'");
        }

        [TestCase("abcd", "'abcd'")]
        [TestCase("test' OR '1' == '1", "'test\\' OR \\'1\\' == \\'1'")]
        [TestCase("test\\", "'test\\\\'")]
        [TestCase("some\0value", "'some\\\\0value'")]
        [TestCase(1, "1")]
        [TestCase(15000000000L, "15000000000")]
        [TestCase(1.123, "1.123")]
        [TestCase(1.123, "1.123")]
        [TestCase(5.75F, "5.75")]
        [TestCase(null, "NULL")]
        [TestCase(true, "True")]
        [TestCase(false, "False")]
        [TestCase(new byte[0], "'\\x'::BYTEA")]
        [TestCase(new byte[] { 1, 2, 3 }, "'\\x01\\x02\\x03'::BYTEA")]
        public void SetParamTest(object paramValue, object expect)
        {
            MockClient client = new MockClient("");
            var cs = new FireboltCommand(new FireboltConnection(mockConnectionString) { Client = client, EngineUrl = "engine" }, "@param", new FireboltParameterCollection());
            cs.Parameters.Add(new FireboltParameter("@param", paramValue));
            cs.ExecuteNonQuery();
            Assert.That(client.Query, Is.EqualTo(expect));
        }

        [TestCase("TimestampTz", "2022-05-10 21:01:02.12345+00", "TimestampTz", "2022-05-10 21:01:02Z", 1234500, TestName = "TimestampTzWithNotAllMicrosecondsWithSecondsInTzTest")]
        [TestCase("TimestampTz", "2022-05-10 21:01:02.0+00", "TimestampTz", "2022-05-10 21:01:02Z", 0, TestName = "TimestampTzWithNotAllMicrosecondsWithSecondsInTzTest")]
        [TestCase("TimestampTz", "1111-01-05 17:04:42+05:53:28", "TimestampTz", "1111-01-05 11:11:14Z", 0, TestName = "TimestampTzWithNotAllMicrosecondsWithSecondsInTzTest")]
        [TestCase("TimestampTz", "1111-01-05 17:04:42.123+05:53:28", "TimestampTz", "1111-01-05 11:11:14.123Z", 0, TestName = "TimestampTzWithNotAllMicrosecondsWithSecondsInTzTest")]
        [TestCase("TimestampTz", "1111-01-05 17:04:42.123456+05:53:28", "TimestampTz", "1111-01-05 11:11:14.123456Z", 0, TestName = "TimestampTzWithNonUnixTimestamp")]
        public void TimestampTest(string type, string timestamp, string expType, string expectedTimestamp, int expTicks)
        {
            TimestampTest(type, timestamp, expType, DateTime.Parse(expectedTimestamp).AddTicks(expTicks));
        }

        [TestCase("TimestampNtz", "2022-05-10 23:01:02.123456", "TimestampNtz", 2022, 5, 10, 23, 1, 2, 0, 1234560)]
        [TestCase("PGDate", "2022-05-10", "Date", 2022, 5, 10, 0, 0, 0, 0, 0)]
        [TestCase("TimestampNtz", "0001-05-10 23:01:02.123", "TimestampNtz", 1, 5, 10, 23, 1, 2, 123, 0, TestName = "TimestampNtzWithNonUnixTimestamp")]
        public void TimestampTest(string type, string timestamp, string expType, int year, int month, int day, int hour, int minute, int second, int millisecond, int ticks)
        {
            TimestampTest(type, timestamp, expType, new DateTime(year, month, day, hour, minute, second, millisecond).AddTicks(ticks));
        }

        private void TimestampTest(string type, string timestamp, string expType, DateTime expectedTimestamp)
        {
            var json = "{\"query\":{\"query_id\": \"173B1C44D6EB9C33\"},\"meta\":[{\"name\": \"test\",\"type\": \"" + type + "\"}],\"data\":[[\"" + timestamp + "\"]],\"rows\": 1}";
            QueryResult res = TypesConverter.ParseJsonResponse(json)!;
            AssertQueryResult(res, expectedTimestamp, expType);
        }

        [TestCase("Nullable(Nothing)", "Null")]
        [TestCase("Nullable(TimestampTz)", "TimestampTz")]
        [TestCase("int null", "Int")]
        [TestCase("string null", "String")]
        public void NullTest(string type, string expectedType)
        {
            var json = "{\"query\":{\"query_id\": \"173B1C44D6EB9C33\"},\"meta\":[{\"name\": \"test\",\"type\": \"" + type + "\"}],\"data\":[[null]],\"rows\": 1}";
            AssertQueryResult(TypesConverter.ParseJsonResponse(json)!, null, expectedType);
        }

        [TestCase("true", true)]
        [TestCase("false", false)]
        public void BooleanTest(string data, bool expect)
        {
            var responseWithPgDate =
                "{\"query\":{\"query_id\": \"1739956EA85D7647\"},\"meta\":[{\"name\": \"CAST(1 AS boolean)\",\"type\": \"boolean\"}],\"data\":[[\"" +
                data +
                "\"]],\"rows\": 1,\"statistics\":{\"elapsed\": 0.001887076,\"rows_read\": 1,\"bytes_read\": 1,\"time_before_execution\": 0.000528582,\"time_to_execute\": 0.000203717,\"scanned_bytes_cache\": 0,\"scanned_bytes_storage\": 0}}";
            QueryResult res = TypesConverter.ParseJsonResponse(responseWithPgDate)!;
            AssertQueryResult(res, expect, "Boolean");
        }

        [Test]
        public void NullBooleanTest()
        {
            var responseWithPgDate =
                "{\"query\":{\"query_id\": \"1739956EA85D7647\"},\"meta\":[{\"name\": \"CAST(1 AS boolean)\",\"type\": \"boolean null\"}],\"data\":[[null]],\"rows\": 1,\"statistics\":{\"elapsed\": 0.001887076,\"rows_read\": 1,\"bytes_read\": 1,\"time_before_execution\": 0.000528582,\"time_to_execute\": 0.000203717,\"scanned_bytes_cache\": 0,\"scanned_bytes_storage\": 0}}";
            QueryResult res = TypesConverter.ParseJsonResponse(responseWithPgDate)!;
            AssertQueryResult(res, null, "Boolean");
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
            command.Transaction = transaction.Object;
            Assert.That(command.Transaction, Is.SameAs(transaction.Object));
        }

        [Test]
        public void CommandTimeout()
        {
            DbCommand command = new FireboltCommand();
            Assert.That(command.CommandTimeout, Is.EqualTo(30));
            command.CommandTimeout = 12345;
            Assert.That(command.CommandTimeout, Is.EqualTo(12345));
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

        [Test]
        public void SetDesignTimeVisible()
        {
            DbCommand command = new FireboltCommand();
            Assert.True(command.DesignTimeVisible); // the default
            // change the value and validate it
            command.DesignTimeVisible = false;
            Assert.False(command.DesignTimeVisible);
            // restore the value and validate again
            command.DesignTimeVisible = true;
            Assert.True(command.DesignTimeVisible);
        }

        [Test]
        public void Cancel()
        {
            DbCommand command = new FireboltCommand();
            Assert.DoesNotThrow(() => command.Cancel()); // just should pass
        }

        [TestCase("select 1", "int", "[[1]]", 1, 1)]
        [TestCase("select 'hello'", "text", "[[\"hello\"]]", 1, "hello")]
        [TestCase("select 1", "text", "[[null]]", 1, null)]
        [TestCase("select 1", "text", "[]", 0, null)]
        public void ExecuteScalar(string query, string type, string data, int rows, object expectedValue)
        {
            string response =
            "{\"query\":{\"query_id\": \"1\"},\"meta\":[{\"name\": \"x\",\"type\": \"" + type + "\"}], \"data\":" + data + ",\"rows\": " + rows + "}";
            var cs = createCommand(query, response);
            Assert.That(cs.ExecuteScalar(), Is.EqualTo(expectedValue));
        }

        [Test]
        public void Prepare()
        {
            DbCommand command = new FireboltCommand();
            Assert.DoesNotThrow(() => command.Prepare()); // just should pass
        }

        private DbCommand createCommand(string? query, string? response)
        {
            return new FireboltCommand(new FireboltConnection(mockConnectionString) { Client = new MockClient(response), EngineUrl = "engine" }, query, new FireboltParameterCollection());
        }

        private void AssertQueryResult(QueryResult result, object? expectedValue, string expectedType, int line = 0, int column = 0)
        {
            var columnType = ColumnType.Of(TypesConverter.GetFullColumnTypeName(result.Meta[column]));
            var convertedValue = TypesConverter.ConvertToCSharpVal(result.Data[line][column]?.ToString(), columnType);
            Assert.That(convertedValue, Is.EqualTo(expectedValue));
            Assert.That(columnType.Type.ToString, Is.EqualTo(expectedType));
        }
    }
}