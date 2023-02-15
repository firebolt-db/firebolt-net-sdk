using System.Net;
using FireboltDotNetSdk.Exception;

namespace FireboltDotNetSdk.Tests;

public class FireboltExceptionTest
{

    [Test]
    public void ThrowExceptionWithStatusCodeInternalServerError()
    {

        FireboltException exception = Assert.Throws<FireboltException>(() => throw new FireboltException(HttpStatusCode.InternalServerError, "my error message from the server"));
        Assert.That(exception.Message, Is.EqualTo("Received an unexpected status code from the server\nStatus: 500\nResponse:\nmy error message from the server"));
        Assert.That(exception.ToString(), Does.Contain("HTTP Response: my error message from the server\nFireboltDotNetSdk.Exception.FireboltException: Received an unexpected status code from the server"));
    }


    [Test]
    public void ThrowExceptionWithStatusCodeInternalServerErrorWithoutServerResponse()
    {
        FireboltException exception = Assert.Throws<FireboltException>(() => throw new FireboltException(HttpStatusCode.InternalServerError, null));
        Assert.That(exception.Message, Is.EqualTo("Received an unexpected status code from the server\nStatus: 500"));
        Assert.That(exception.ToString(), Does.Contain("FireboltDotNetSdk.Exception.FireboltException: Received an unexpected status code from the server"));
    }

    [Test]
    public void ThrowExceptionWithStatusCodeUnauthorized()
    {
        FireboltException exception = Assert.Throws<FireboltException>(() => throw new FireboltException(HttpStatusCode.Unauthorized, "my error message from the server"));
        Assert.That(exception.Message, Is.EqualTo("The operation is unauthorized\nStatus: 401\nResponse:\nmy error message from the server"));
        Assert.That(exception.ToString(), Does.Contain("HTTP Response: my error message from the server\nFireboltDotNetSdk.Exception.FireboltException: The operation is unauthorized"));
    }

    [Test]
    public void ThrowExceptionWithStatusCodeForbidden()
    {

        FireboltException exception = Assert.Throws<FireboltException>(() => throw new FireboltException(HttpStatusCode.Forbidden, "my error message from the server"));
        Assert.That(exception.Message, Is.EqualTo("The operation is forbidden\nStatus: 403\nResponse:\nmy error message from the server"));
        Assert.That(exception.ToString(), Does.Contain("HTTP Response: my error message from the server\nFireboltDotNetSdk.Exception.FireboltException: The operation is forbidden"));
    }

    [Test]
    public void ThrowExceptionWithoutStatusCode()
    {
        FireboltException exception = Assert.Throws<FireboltException>(() => throw new FireboltException("An error happened"));
        Assert.That(exception.Message, Is.EqualTo("An error happened"));
        Assert.That(exception.ToString(), Does.Not.Contain("HTTP Response:"));
    }





}