namespace FireboltDotNetSdk.Tests;

public class HttpClientSingletonTest
{
    [Test]
    public void InitSingletonOnlyOnce()
    {
        HttpClient client = HttpClientSingleton.GetInstance();
        HttpClient client2 = HttpClientSingleton.GetInstance();
        Assert.True(ReferenceEquals(client, client2));
    }

    [Test]
    public void SetDefaultRequestHeaders()
    {
        HttpClient client = HttpClientSingleton.GetInstance();
        Assert.That(client.DefaultRequestHeaders.UserAgent.ToList()[0].Product.Name, Is.EqualTo(".NETSDK"));
    }
}