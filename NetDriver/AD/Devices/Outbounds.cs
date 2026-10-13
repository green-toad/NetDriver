using System.Net.Sockets;
using System.Threading.Channels;

namespace NetDriver.AD.Devices
{
    internal class OutboundDevice : IAsyncDisposable
    {
        private readonly Socket _socket;
        private readonly Channel<Cat> _outChan;
        private readonly CancellationTokenSource _cts;
        private readonly DisconnectLink _disconnect;
        private readonly Task _writer;
        public OutboundDevice(IComposer composer)
        {
            _socket = composer.Socket;
            _outChan = composer.Outbounds;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(composer.Ct);
            _disconnect = composer.DisconnectEvent;
            _writer = Task.Run(Writing);
        }

        private async Task Writing()
        {
            try
            {
                await foreach (var cat in _outChan.Reader.ReadAllAsync(_cts.Token))
                {
                    var a = cat.Compress();
                    await _socket.SendAsync(a, _cts.Token);
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
