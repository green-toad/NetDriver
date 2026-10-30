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

        public ValueTask DisposeAsync()
        {
            throw new NotImplementedException();
        }

        private async Task Decrypting()
        {
            while (!_cts.IsCancellationRequested)
            {
                var income = await _composer.Inbounds.Reader.ReadAsync();
                var bytes = income.Buffer.ToArray();

                var mem = _composer.PlainText.Writer.GetMemory(bytes.Length);
                // Buffer.BlockCopy(bytes, 0, mem, 0, bytes.Length);
            }
        }
    }
}
