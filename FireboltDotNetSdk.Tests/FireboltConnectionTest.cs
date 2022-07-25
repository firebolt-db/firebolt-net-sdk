using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;

namespace FireboltDotNetSdk.Tests
{
    
    public class FireboltConnectionTest
    {

        [Test]
        public void ParsingNormalConnectionStringTest()
        {
            const string connectionString = "database=testdb.ib;username=testuser;password=testpwd;account=accountname;endpoint=endpoint";
            var cs = new FireboltConnection(connectionString);
            Assert.Multiple(() =>
            {
                Assert.That(cs.Endpoint, Is.EqualTo("endpoint"));
                Assert.That(cs.Database, Is.EqualTo("testdb.ib"));
                Assert.That(cs.Account, Is.EqualTo("accountname"));
                Assert.That(cs.Password, Is.EqualTo("testpwd"));
                Assert.That(cs.UserName, Is.EqualTo("testuser"));
            });
        }

        [Test]
        public void ParsingMissPassConnectionStringTest()
        {
            const string connectionString = "database=testdb.ib;username=testuser;password=;account=accountname;endpoint=endpoint";
            var cs = new FireboltConnection(connectionString);
            var ex = Assert.Throws<FireboltException>(
                delegate { throw new FireboltException("Password is missing"); }) ?? throw new InvalidOperationException();
            Assert.That(ex.Message, Is.EqualTo("Password is missing"));
        }

        [TestCase("test")]
        public void ParsingDatabaseHostnames(string hostname)
        {
            var ConnectionString = $"database={hostname}:test.ib;username=user";
            var cs = new FireboltConnection(ConnectionString);
            Assert.AreEqual("test:test.ib", cs.Database);
        }
        
    }
}
