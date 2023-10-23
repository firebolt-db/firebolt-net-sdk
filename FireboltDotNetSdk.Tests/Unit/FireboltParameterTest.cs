using FireboltDotNetSdk.Client;
using System.Data.Common;
using System.Data;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    public class FireboltParameterTest
    {
        [Test]
        public void CreateDefaultParameter()
        {
            DbParameter parameter = new FireboltParameter();
            Assert.That(parameter.ParameterName, Is.EqualTo("parameter"));
            Assert.That(parameter.Value, Is.EqualTo(null));
            Assert.True(parameter.SourceColumnNullMapping);
        }

        [Test]
        public void CreateParameterWithNameAndValue()
        {
            DbParameter parameter = new FireboltParameter("one", 1);
            Assert.That(parameter.ParameterName, Is.EqualTo("one"));
            Assert.That(parameter.Value, Is.EqualTo(1));
            Assert.True(parameter.SourceColumnNullMapping);
        }

        [Test]
        public void CreateParameterWithNameValueAndType()
        {
            DbParameter parameter = new FireboltParameter("number", DbType.Int16, 123);
            Assert.That(parameter.ParameterName, Is.EqualTo("number"));
            Assert.That(parameter.Value, Is.EqualTo(123));
            Assert.That(parameter.DbType, Is.EqualTo(DbType.Int16));
            Assert.True(parameter.SourceColumnNullMapping);
        }

        [Test]
        public void CreateParameterWithNameValueAndNullType()
        {
            DbParameter parameter = new FireboltParameter("number", null, 123);
            Assert.That(parameter.ParameterName, Is.EqualTo("number"));
            Assert.That(parameter.Value, Is.EqualTo(123));
            Assert.That(parameter.DbType, Is.EqualTo(DbType.Int32));
            Assert.True(parameter.SourceColumnNullMapping);
        }

        [Test]
        public void CreateNotNullableParameter()
        {
            DbParameter parameter = new FireboltParameter() { SourceColumnNullMapping = false };
            Assert.That(parameter.ParameterName, Is.EqualTo("parameter"));
            Assert.That(parameter.Value, Is.EqualTo(null));
            Assert.False(parameter.SourceColumnNullMapping);
        }

        [TestCase(DbType.Currency)]
        [TestCase(DbType.VarNumeric)]
        [TestCase(DbType.AnsiStringFixedLength)]
        [TestCase(DbType.StringFixedLength)]
        [TestCase(DbType.Xml)]
        [TestCase(DbType.DateTime2)]
        [TestCase(DbType.Single)]
        public void CreateParameterWithUnsupportedType(DbType dbType)
        {
            Assert.Throws<NotSupportedException>(() => new FireboltParameter("parameter", dbType, null));
        }

        [Test]
        public void SourceColumn()
        {
            DbParameter parameter = new FireboltParameter("one", 1);
            Assert.That(parameter.SourceColumn, Is.EqualTo(string.Empty));
            parameter.SourceColumn = "my_column";
            Assert.That(parameter.SourceColumn, Is.EqualTo("my_column"));
        }

        [Test]
        public void SetValue()
        {
            DbParameter parameter = new FireboltParameter();
            Assert.That(parameter.Value, Is.EqualTo(null));
            Assert.That(parameter.Size, Is.EqualTo(0));

            parameter.Value = "hello";

            Assert.That(parameter.Value, Is.EqualTo("hello"));
            Assert.That(parameter.Size, Is.EqualTo(int.MaxValue));

            parameter.Size = 10;
            Assert.That(parameter.Size, Is.EqualTo(10));
        }

        [Test]
        public void IsNullable()
        {
            DbParameter parameter = new FireboltParameter("one", "first");
            Assert.That(parameter.IsNullable, Is.EqualTo(true));
            parameter.IsNullable = false;
            Assert.That(parameter.IsNullable, Is.EqualTo(false));

            parameter.IsNullable = true;
            Assert.That(parameter.IsNullable, Is.EqualTo(true));

            parameter.SourceColumnNullMapping = true;
            parameter.IsNullable = true;
            Assert.That(parameter.IsNullable, Is.EqualTo(true));

            parameter.SourceColumnNullMapping = false;
            Assert.Throws<ArgumentException>(() => parameter.IsNullable = true);
        }

        [TestCase(DbType.String, "hello", 0)]
        [TestCase(DbType.Decimal, 3.14, 15)]
        public void Scale(DbType type, object value, int expectedScale)
        {
            DbParameter parameter = new FireboltParameter("param", type, value);
            Assert.That(parameter.Scale, Is.EqualTo(expectedScale));
            Assert.Throws<NotImplementedException>(() => parameter.Scale = 0);
        }

        [Test]
        public void ResetDbType()
        {
            DbParameter parameter = new FireboltParameter("param", DbType.Int16, 123);
            Assert.That(parameter.DbType, Is.EqualTo(DbType.Int16));
            parameter.DbType = DbType.Int32;
            Assert.That(parameter.DbType, Is.EqualTo(DbType.Int32));
            parameter.ResetDbType();
            Assert.That(parameter.DbType, Is.EqualTo(DbType.Int16));
        }

        [Test]
        public void SetParameterDirection()
        {
            DbParameter parameter = new FireboltParameter("param", 123);
            Assert.That(parameter.Direction, Is.EqualTo(ParameterDirection.Input));
            parameter.Direction = ParameterDirection.Input;
            Assert.That(parameter.Direction, Is.EqualTo(ParameterDirection.Input));
            Assert.Throws<NotSupportedException>(() => parameter.Direction = ParameterDirection.Output);
        }

        [Test]
        public void SetParameterName()
        {
            DbParameter parameter = new FireboltParameter("one", 123);
            Assert.That(parameter.ParameterName, Is.EqualTo("one"));
            parameter.ParameterName = "two";
            Assert.That(parameter.ParameterName, Is.EqualTo("two"));
            Assert.Throws<ArgumentNullException>(() => parameter.ParameterName = null);
        }
    }

}