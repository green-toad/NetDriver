using System.Net.Sockets;
using System.Threading.Channels;

namespace NetDriver.AD.Devices
{
    internal class OutboundDevice : IAsyncDisposable
    {
        private readonly CancellationTokenSource _cts;
        private readonly Task _writer;

        private readonly IComposer _composer;
        public OutboundDevice(IComposer composer)
        {
            _cts = CancellationTokenSource.CreateLinkedTokenSource(composer.Ct);
            _composer = composer;
            _writer = Task.Run(Writing);
        }

        private async Task Writing()
        {
            try
            {
                await foreach (var cat in _composer.Outbounds.Reader.ReadAllAsync(_cts.Token))
                {
                    var a = cat.Compress();
                    await _composer.Socket.SendAsync(a, _cts.Token);
                }
            }
            catch (OperationCanceledException) { }
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                _cts.Cancel();
                await _writer;
                _cts.Dispose();
            }
            catch
            {
                Console.Write("какая то фигня при освобождении ресурсов в Outbounds\n");
            }
        }
    }
}
