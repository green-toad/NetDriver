using System;
using System.Buffers;
using System.IO.Pipelines;

namespace NetDriver.AD.Devices
{
    internal class ProcessingCenterDevice : IAsyncDisposable
    {
        private readonly IComposer _composer;
        private readonly Task _handleIncoming;
        private readonly CancellationTokenSource _cts;
        public ProcessingCenterDevice(IComposer composer)
        {
            _composer = composer;
            _handleIncoming = Task.Run(HandleIncoming);
            _cts = CancellationTokenSource.CreateLinkedTokenSource(composer.Ct);
        }

        private async Task HandleIncoming()
        {
            while (!_cts.IsCancellationRequested)
            {
                ReadResult result = await _composer.Inbounds.Reader.ReadAsync(_cts.Token);
                ReadOnlySequence<byte> buffer = result.Buffer;
                var seq = new SequenceReader<byte>(buffer);
                byte[] head = new byte[21];
                try // читаем голову кота (=^･ω･^=)
                {
                    UInt16 counter = 0;
                    while (seq.TryPeek(out byte encryptedByte))
                    {
                        byte[] decrypted = await _composer.DecFunc(new[] { encryptedByte });

                        seq.Advance(1);

                        if (decrypted.Length == 1)
                        {
                            head[counter] = decrypted[0];
                            counter++;
                        }
                    }
                }
                finally
                {
                    _composer.Inbounds.Reader.AdvanceTo(seq.Position, buffer.End);
                }
            }
        }

        public ValueTask DisposeAsync()
        {
            throw new NotImplementedException();
        }
    }
}
