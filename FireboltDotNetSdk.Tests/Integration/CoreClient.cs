using FireboltDotNetSdk.Client;
using static FireboltDotNetSdk.Client.FireResponse;

namespace FireboltDotNetSdk.Tests
{
    internal class CoreClient : FireboltClient
    {
        private static readonly HttpClient CoreHttpClient = new();
        private readonly Uri _coreUri;

        public CoreClient(FireboltConnection connection, Uri coreUri) : base(connection, "core", "core", "", null, null, CoreHttpClient)
        {
            _coreUri = coreUri;
        }

        // The SDK always builds https URLs without a port; redirect them to Core and skip the bearer token.
        protected override async Task<HttpRequestMessage> GetHttpRequest(HttpMethod method, string uri, HttpContent? content, bool needsAccessToken)
        {
            var request = await base.GetHttpRequest(method, uri, content, needsAccessToken: false);
            request.RequestUri = new UriBuilder(request.RequestUri!) { Scheme = _coreUri.Scheme, Host = _coreUri.Host, Port = _coreUri.Port }.Uri;
            TestContext.Progress.WriteLine($"{TestContext.CurrentContext.Test.Name}: request URL is {request.RequestUri.AbsoluteUri.Length} chars");
            return request;
        }

        protected override Task<LoginResponse> Login(string id, string secret, string env) => throw new NotSupportedException();

        public override Task<GetAccountIdByNameResponse> GetAccountIdByNameAsync(string account, CancellationToken cancellationToken) =>
            Task.FromResult(new GetAccountIdByNameResponse { infraVersion = 2 });

        public override Task<ConnectionResponse> ConnectAsync(string? engineName, string database, CancellationToken cancellationToken) => throw new NotSupportedException();

        internal override void CleanupCache()
        {
        }
    }
}
