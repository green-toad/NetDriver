using System;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;

namespace NetDriver.AD.Devices
{
    // он должен принимать трафик и вкидывать его в очередь на обработку
    internal class InboundDevice : IAsyncDisposable
    {
        private readonly CancellationTokenSource _cts;
        private readonly Task _reader;
        private readonly IComposer _composer;

        public InboundDevice(IComposer composer)
        {
            _cts = CancellationTokenSource.CreateLinkedTokenSource(composer.Ct);
            _composer = composer;
            _reader = Task.Run(Reading);
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                _cts.Cancel();
                await _reader;
                _cts.Dispose();
            }
            catch
            {
                Console.Write("какая то фигня при освобождении ресурсов в Inbounds\n");
            }
        }

        private async Task Reading()
        {
            try
            {
                while (!_cts.IsCancellationRequested)
                {
                    Memory<byte> buffer = _composer.Inbounds.Writer.GetMemory(4096);
                    var recivedLenght = await _composer.Socket.ReceiveAsync(buffer, _cts.Token);

                    if (recivedLenght == 0)
                    {
                        _composer.DisconnectEvent(_composer.Socket); // токен отменится при вызове поэтому break не нужен
                        break;
                    }
                    _composer.Inbounds.Writer.Advance(recivedLenght);
                    await _composer.Inbounds.Writer.FlushAsync(_cts.Token);
                }
            }
            catch (OperationCanceledException) { }
        }
    }
}
