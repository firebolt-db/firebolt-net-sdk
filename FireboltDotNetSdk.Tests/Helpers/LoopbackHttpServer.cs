using System.Net;
using System.Net.Sockets;
using System.Text;

namespace FireboltDotNetSdk.Tests.Helpers
{
    /// <summary>
    /// A plain HTTP/1.1 server on localhost serving one request, where the test writes the response bytes itself.
    /// Lets tests exercise a real SocketsHttpHandler, including stalls and compressed chunks a mocked handler cannot.
    /// </summary>
    internal sealed class LoopbackHttpServer : IAsyncDisposable
    {
        private readonly TcpListener _listener;
        private readonly Task<TcpClient> _accepted;
        private readonly CancellationTokenSource _stop = new();

        public LoopbackHttpServer()
        {
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
            _accepted = _listener.AcceptTcpClientAsync();
            RequestHead = ReadRequestHead();
        }

        public string Url => $"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}/";

        public Task<string> RequestHead { get; }

        public async Task WriteAsync(string text)
        {
            await WriteAsync(Encoding.UTF8.GetBytes(text));
        }

        public async Task WriteAsync(byte[] bytes)
        {
            var client = await _accepted;
            await client.GetStream().WriteAsync(bytes, _stop.Token);
        }

        public async Task WriteChunkAsync(byte[] bytes)
        {
            await WriteAsync($"{bytes.Length:X}\r\n");
            await WriteAsync(bytes);
            await WriteAsync("\r\n");
        }

        private async Task<string> ReadRequestHead()
        {
            var client = await _accepted;
            var head = new StringBuilder();
            var buffer = new byte[1];
            while (!head.ToString().EndsWith("\r\n\r\n") && await client.GetStream().ReadAsync(buffer, _stop.Token) > 0)
                head.Append((char)buffer[0]);
            return head.ToString();
        }

        public async ValueTask DisposeAsync()
        {
            _stop.Cancel();
            _listener.Stop();
            try
            {
                (await _accepted).Dispose();
            }
            catch (System.Exception)
            {
                // Nothing connected.
            }
            _stop.Dispose();
        }
    }
}
