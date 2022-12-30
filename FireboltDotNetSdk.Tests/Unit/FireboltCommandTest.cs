using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    public class FireboltCommandTest
    {
        private FireboltCommand _fireboltCommand;

        [SetUp]
        public void Init()
        {
            _fireboltCommand = new FireboltCommand();
            _fireboltCommand.Response =
                "{\"query\":{\"query_id\": \"16FDB86662938757\"},\"meta\":[{\"name\": \"uint8\",\"type\": \"UInt8\"}],\"data\":[[1]],\"rows\": 1,\"statistics\":{\"elapsed\": 0.000620069,\"rows_read\": 1,\"bytes_read\": 1,\"time_before_execution\": 0.000409657,\"time_to_execute\": 0.000208377,\"scanned_bytes_cache\": 0,\"scanned_bytes_storage\": 0}}";
        }

        [TestCase("SET param=1")]
        [TestCase("SET param=1,param=2")]
        public void ExecuteTest(string commandText)
        {
            var cs = new FireboltCommand();
            cs.Execute(commandText);
            Assert.IsNotEmpty(cs.SetParamList);
        }

        [TestCase("Select 1")]
        public void ExecuteWrongWaySelectTest(string commandText)
        {
            var cs = new FireboltCommand();
            try
            {
                var t = cs.Execute(commandText);
            }
            catch (FireboltException ex)
            {
                Assert.That(ex.Message, Is.EqualTo("Response is empty while GetOriginalJSONData"));
            }
        }

        [Test]
        public void GetOriginalJsonDataExceptionTest()
        {
            var cs = new FireboltCommand();
            try
            {
                cs.GetOriginalJsonData();
            }
            catch (FireboltException ex)
            {
                Assert.That(ex.Message, Is.EqualTo("Response is empty while GetOriginalJSONData"));
            }
        }

        [Test]
        public void GetOriginalJsonDataTest()
        {
            var result = _fireboltCommand.GetOriginalJsonData();
            Assert.IsNotNull(result.Data.Count);
        }

        [Test]
        public void RowCountTest()
        {
            var result = _fireboltCommand.RowCount();
            Assert.IsNotNull(result);
        }

        [Test]
        public void RowCountExcecptionTest()
        {
            var cs = new FireboltCommand();
            try
            {
                cs.RowCount();
            }
            catch (FireboltException ex)
            {
                Assert.That(ex.Message, Is.EqualTo("RowCount is missing"));
            }
        }

        [TestCase("SET param=1")]
        [TestCase("SET param=1,param=2")]
        public void ClearSetListTest(string commandText)
        {
            var cs = new FireboltCommand();
            cs.Execute(commandText);
            Assert.IsNotEmpty(cs.SetParamList);
            cs.ClearSetList();
            Assert.IsEmpty(cs.SetParamList);
        }

        [Test]
        public void FormDataForResponseTest()
        {
            var result = FireboltCommand.FormDataForResponse(_fireboltCommand.Response);
            Assert.IsNotNull(result);
        }

        [Test]
        public void FormDataForResponseInvalidTest()
        {
            try
            {
                FireboltCommand.FormDataForResponse(null);
            }
            catch (FireboltException e)
            {
                Assert.That(e.Message, Is.EqualTo("JSON data is missing"));
            }
        }

        [TestCase("abcd")]
        public void ExistaramListTest(string commandText)
        {
            var cs = new FireboltCommand();
            cs.Parameters.AddWithValue("@param", commandText);
            Assert.IsTrue(cs.Parameters.Any());
        }


        [TestCase("abcd", "'abcd'")]
        [TestCase("test' OR '1' == '1", "'test\\' OR \\'1\\' == \\'1'")]
        [TestCase("test\\", "'test\\\\'")]
        [TestCase("some\0value", "'some\\\\0value'")]
        public void SetParamListStrTest(string commandText, string expect)
        {
            var testParam = "@param";
            var cs = new FireboltCommand();
            cs.Parameters.AddWithValue("@param", commandText);
            var result = cs.GetParamQuery(testParam);

            Assert.That(expect, Is.EqualTo(result));
        }

        [TestCase(1, "1")]
        public void SetParamListIntTest(int commandText, string expect)
        {
            var testParam = "@param";
            var cs = new FireboltCommand();
            cs.Parameters.AddWithValue("@param", commandText);
            var result = cs.GetParamQuery(testParam);

            Assert.That(expect, Is.EqualTo(result));
        }

        [TestCase(15000000000L, "15000000000")]
        public void SetParamListLongTest(long commandText, string expect)
        {
            var testParam = "@param";
            var cs = new FireboltCommand();
            cs.Parameters.AddWithValue("@param", commandText);
            var result = cs.GetParamQuery(testParam);

            Assert.That(expect, Is.EqualTo(result));
        }

        [TestCase(1.123, "1.123")]
        public void SetParamListDoubleTest(double commandText, string expect)
        {
            var testParam = "@param";
            var cs = new FireboltCommand();
            cs.Parameters.AddWithValue("@param", commandText);
            var result = cs.GetParamQuery(testParam);

            Assert.That(expect, Is.EqualTo(result));
        }

        [TestCase(1.123, "1.123")]
        public void SetParamListDecimalTest(Decimal commandText, string expect)
        {
            var testParam = "@param";
            var cs = new FireboltCommand();
            cs.Parameters.AddWithValue("@param", commandText);
            var result = cs.GetParamQuery(testParam);

            Assert.That(expect, Is.EqualTo(result));
        }

        [TestCase(5.75F, "5.75")]
        public void SetParamListFloatTest(float commandText, string expect)
        {
            var testParam = "@param";
            var cs = new FireboltCommand();
            cs.Parameters.AddWithValue("@param", commandText);
            var result = cs.GetParamQuery(testParam);

            Assert.That(expect, Is.EqualTo(result));
        }

        [TestCase(null, "NULL")]
        public void SetParamListNullTest(string commandText, string expect)
        {
            var testParam = "@param";
            var cs = new FireboltCommand();
            cs.Parameters.AddWithValue("@param", commandText);
            var result = cs.GetParamQuery(testParam);

            Assert.That(expect, Is.EqualTo(result));
        }

        [Test]
        public void SetParamListDatesTest()
        {
            var testParam = "@param";
            var commandText = DateTime.Now;
            var cs = new FireboltCommand();
            cs.Parameters.AddWithValue("@param", commandText);
            var result = cs.GetParamQuery(testParam);
            var expect = commandText.ToString("yyyy-MM-dd HH:mm:ss");
            Assert.That("'" + expect + "'", Is.EqualTo(result));
        }
    }
}
