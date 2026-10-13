using System;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;

namespace NetDriver.AD.Devices
{
    // он должен принимать трафик и вкидывать его в очередь на обработку
    internal class InboundDevice : IAsyncDisposable
    {
        private readonly PipeWriter _writer;
        private readonly CancellationTokenSource _cts;
        private readonly Task _reader;
        private readonly Socket _socket;
        private readonly DisconnectLink _disconnect;

        public InboundDevice(IComposer composer)
        {
            _writer = composer.Inbounds;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(composer.Ct);
            _disconnect = composer.DisconnectEvent;
            _socket = composer.Socket;

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
                    Memory<byte> buffer = _writer.GetMemory(4096);
                    var recivedLenght = await _socket.ReceiveAsync(buffer, _cts.Token);

                    if (recivedLenght == 0)
                    {
                        _disconnect(_socket); // токен отменится при вызове поэтому break не нужен
                    }
                    await _writer.FlushAsync(_cts.Token);
                }
            }
            catch (OperationCanceledException) { }
        }
    }
}
