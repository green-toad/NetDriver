using System;
using System.Buffers;

namespace NetDriver.AD.Devices
{
    internal class DecryptorDevice : IAsyncDisposable
    {
        private readonly CancellationTokenSource _cts;
        private readonly IComposer _composer;
        private readonly Task _decryptor;

        public DecryptorDevice(IComposer composer)
        {
            _composer = composer;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(_composer.Ct);
            _decryptor = Task.Run(Decrypting);
        }

        public async ValueTask DisposeAsync()
        {
            _cts.Cancel();
            await _decryptor;
            _cts.Dispose();
        }

        private async Task Decrypting()
        {
            while (!_cts.IsCancellationRequested)
            {
                try
                {
                    var income = await _composer.Inbounds.Reader.ReadAsync();
                    var bytes = income.Buffer.ToArray();
                    _composer.Inbounds.Reader.AdvanceTo(income.Buffer.End);

                    if (bytes.Length > 0)
                    {
                        byte[] result = await _composer.DecFunc(bytes);

                        if (result.Length > 0)
                        {
                            Memory<byte> mem = _composer.Decryptor.Writer.GetMemory(result.Length);
                            result.AsSpan().CopyTo(mem.Span);
                            _composer.Decryptor.Writer.Advance(result.Length);
                        }
                    }

                    var flush = await _composer.Decryptor.Writer.FlushAsync(_cts.Token);

                    if (flush.IsCompleted || flush.IsCanceled) break;
                }
                catch (OperationCanceledException) { }
                catch (Exception e)
                {
                    _composer.Loger.Log(this, e);
                }
            }
        }
    }
}
