using NetDriver.AE;
using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;

namespace NetDriver.AE
{
    internal class OutcomingController : IAsyncDisposable
    {
        private readonly Socket socket;
        private readonly Task writing;
        private readonly Channel<byte[]> outcomingBuffer = Channel.CreateUnbounded<byte[]>();
        private readonly CancellationTokenSource _cts = new();
        private readonly EncryptMethod? _enc;

        public OutcomingController(Socket sock, EncryptMethod? enc)
        {
            socket = sock;
            writing = Sending();
            _enc = enc;
        }

        private async Task Sending()
        {
            try
            {
                while (!_cts.IsCancellationRequested)
                {
                    var content = await outcomingBuffer.Reader.ReadAsync(_cts.Token);
                    var toSend = _enc == null ? content : await _enc(content);
                    await socket.SendAsync(content);
                }
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                outcomingBuffer.Writer.TryComplete();
            }
        }

        public async Task Send(byte[] content)
        {
            await outcomingBuffer.Writer.WriteAsync(content);
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                _cts.Cancel();
                await writing;
                _cts.Dispose();
            }
            catch (OperationCanceledException) { }
            catch (Exception e) { Console.Write("[outcoming thread] " + e + "\n"); }

        }
    }
}
