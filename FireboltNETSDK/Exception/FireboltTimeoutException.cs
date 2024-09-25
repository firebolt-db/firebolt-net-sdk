namespace FireboltDotNetSdk.Exception;

public class FireboltTimeoutException : FireboltException
{
    public FireboltTimeoutException(int timeoutMillis) : base($"Query execution timeout. The query did not complete within {timeoutMillis} milliseconds.")
    {
    }
}