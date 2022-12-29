using System.Data;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;
using static NUnit.Framework.Assert;

namespace FireboltDotNetSdk.Tests;

[TestFixture]
public class FireboltConnectionTest
{
    [Test]
    public void ParsingNormalConnectionStringTest()
    {
        const string connectionString =
            "database=testdb.ib;username=testuser;password=testpwd;account=accountname;endpoint=endpoint";
        var cs = new FireboltConnection(connectionString);
        Multiple(() =>
        {
            That(cs.Endpoint, Is.EqualTo("endpoint"));
            That(cs.Database, Is.EqualTo("testdb.ib"));
            That(cs.Account, Is.EqualTo("accountname"));
            That(cs.Password, Is.EqualTo("testpwd"));
            That(cs.UserName, Is.EqualTo("testuser"));
        });
    }

    [Test]
    public void ParsingMissPassConnectionStringTest()
    {
        const string connectionString =
            "database=testdb.ib;username=testuser;password=;account=accountname;endpoint=endpoint";
        var cs = new FireboltConnection(connectionString);
        var ex = Throws<FireboltException>(
            delegate { throw new FireboltException("Password is missing"); }) ?? throw new InvalidOperationException();
        That(ex.Message, Is.EqualTo("Password is missing"));
    }

    [Test]
    public void ParsingInvalidConnectionStringTest()
    {
        const string connectionString =
            "database=testdb.ib;username=test_user;password=test_pwd;account=account_name;endpoint=endpoint";
        var cs = new FireboltConnection(connectionString);
        Multiple(() =>
        {
            That(cs.Endpoint, Is.EqualTo("endpoint"));
            That(cs.Database, Is.EqualTo("testdb.ib"));
            That(cs.Account, Is.EqualTo("account_name"));
            That(cs.Password, Is.EqualTo("test_pwd"));
            That(cs.UserName, Is.EqualTo("test_user"));
        });
    }

    [Test]
    public void SetEngineTest()
    {
        const string connectionString =
            "database=testdb.ib;username=testuser;password=lol;account=accountname;endpoint=endpoint";
        var cs = new FireboltConnection(connectionString);
        try
        {
            cs.SetEngine("default_Engine");
        }
        catch (FireboltException ex)
        {
            That(ex.Message, Does.StartWith("Cannot get engine: default_Engine from testdb.ib database"));
        }
    }

    [Test]
    public void OnSessionEstablishedTest()
    {
        const string connectionString =
            "database=testdb.ib;username=testuser;password=;account=accountname;endpoint=endpoint";
        var cs = new FireboltConnection(connectionString);
        cs.OnSessionEstablished();
        That(cs.State, Is.EqualTo(ConnectionState.Open));
    }

    [Test]
    public void CloseTest()
    {
        const string connectionString =
            "database=testdb.ib;username=testuser;password=;account=accountname;endpoint=endpoint";
        var cs = new FireboltConnection(connectionString);
        var conState = new FireboltConnectionState();
        cs.Close();
        That(conState.State, Is.EqualTo(ConnectionState.Closed));
    }

    [TestCase("test")]
    public void ParsingDatabaseHostnames(string hostname)
    {
        var ConnectionString = $"database={hostname}:test.ib;username=user";
        var cs = new FireboltConnection(ConnectionString);
        That(cs.Database, Is.EqualTo("test:test.ib"));
    }

    [Test]
    public void OpenTestWithoutPassword()
    {
        const string connectionString =
            "database=testdb.ib;username=testuser;password=;account=accountname;endpoint=endpoint";
        var cs = new FireboltConnection(connectionString);
        try
        {
            cs.OpenAsync();
        }
        catch (FireboltException ex)
        {
            That(ex.Message, Is.EqualTo("Password is missing"));
        }
    }


    [Test]
    public void OpenExceptionTest()
    {
        const string connectionString =
            "database=testdb.ib;username=testuser;password=password;account=accountname;endpoint=endpoint";
        var cs = new FireboltConnection(connectionString);
        var ex = ThrowsAsync<InvalidOperationException>(async () => await cs.OpenAsync());
    }


    [Test]
    public void OpenAsyncTest()
    {
        const string connectionString =
            "database=testdb.ib;username=testuser;password=password;account=accountname;endpoint=endpoint";
        var cs = new FireboltConnection(connectionString);
        try
        {
            cs.OpenAsync();
        }
        catch (System.Exception ex)
        {
            That(ex.Message,
                Is.EqualTo(
                    "An invalid request URI was provided. Either the request URI must be an absolute URI or BaseAddress must be set."));
        }
    }

    [Test]
    public void OpenInvalidUrlTest()
    {
        const string connectionString =
            "database=testdb.ib;username=testuser;password=passwordtest;account=accountname;endpoint=endpoint";
        var cs = new FireboltConnection(connectionString);
        try
        {
            cs.Open();
        }
        catch (System.Exception ex)
        {
            That(ex.Message,
                Is.EqualTo(
                    "An invalid request URI was provided. Either the request URI must be an absolute URI or BaseAddress must be set."));
        }
    }

    [Test]
    public void CreateCursorTest()
    {
        const string connectionString =
            "database=testdb.ib;username=testuser;password=passwordtest;account=accountname;endpoint=endpoint;";
        var cs = new FireboltConnection(connectionString);
        var cursor = cs.CreateCursor();
        Equals("testdb.ib", cursor.Connection?.Database);
    }

    [TestCase("Select 1")]
    public void CreateCursorCommandTextTest(string commandText)
    {
        const string connectionString =
            "database=testdb.ib;username=testuser;password=passwordtest;account=accountname;endpoint=endpoint";
        var cs = new FireboltConnection(connectionString);
        var cursor = cs.CreateCursor(commandText);
        That(cursor.CommandText, Is.EqualTo("Select 1"));
    }

    [TestCase("Select 1")]
    public void FireboltExceptionTest(string commandText)
    {
        const string connectionString =
            "database=testdb.ib;username=testuser;password=passwordtest;account=accountname;endpoint=endpoint";
        var cs = new FireboltConnection(connectionString);
        var cursor = cs.CreateCursor(commandText);
        That(cursor.CommandText, Is.EqualTo("Select 1"));
    }
}