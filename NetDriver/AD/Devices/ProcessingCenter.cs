using System;
using System.Buffers;
using System.IO.Pipelines;

namespace NetDriver.AD.Devices
{
    internal sealed class ProcessingCenterDevice : IAsyncDisposable
    {
        private readonly IComposer _composer;
        private readonly CancellationTokenSource _cts;
        private readonly Task _handleIncoming;

        private readonly byte[] _oneCipherByte = new byte[1];

        private byte[] _pending = Array.Empty<byte>();
        private int _pendingOffset;
        private int _pendingCount;

        public ProcessingCenterDevice(IComposer composer)
        {
            _composer = composer;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(composer.Ct);
            _handleIncoming = Task.Run(HandleIncoming);
        }

        private async Task HandleIncoming()
        {
            try
            {
                while (!_cts.IsCancellationRequested)
                {
                    var headBytes = new byte[21];
                    await ReadPlaintextExactlyAsync(headBytes, 0, 21, _cts.Token);
                    var head = new Cat.Head(headBytes);

                    var bodyBytes = new byte[head.contentSize];
                    if (bodyBytes.Length > 0)
                        await ReadPlaintextExactlyAsync(bodyBytes, 0, bodyBytes.Length, _cts.Token);

                    var body = new Cat.Body(head, bodyBytes);
                    var cat = new Cat(head, body);

                    switch (cat.head.type)
                    {
                        case Cat.Type.Answer:
                            if (_composer.AnswerRequests.TryRemove(cat.head.uid, out var res))
                            {
                                res.TrySetResult(cat);
                            }
                            break;
                        case Cat.Type.PING:
                            break;
                        default:
                            _composer.ToUserEvent(cat);
                            break;
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch
            {
                Console.Write("ошибка в обработчике [ProcessingCenterDevice]\n");
            }
        }

        private async ValueTask ReadPlaintextExactlyAsync(
            byte[] destination,
            int offset,
            int count,
            CancellationToken ct)
        {
            int written = 0;
            if (_pendingCount > 0)
            {
                int take = Math.Min(count, _pendingCount);
                Buffer.BlockCopy(_pending, _pendingOffset, destination, offset, take);
                _pendingOffset += take;
                _pendingCount -= take;
                written += take;
                if (_pendingCount == 0) _pendingOffset = 0;
            }

            while (written < count)
            {
                var result = await _composer.Inbounds.Reader.ReadAsync(ct);
                var buffer = result.Buffer;
                var seq = new SequenceReader<byte>(buffer);

                try
                {
                    while (written < count && seq.TryPeek(out byte encryptedByte))
                    {
                        _oneCipherByte[0] = encryptedByte;

                        byte[] decrypted = await _composer.DecFunc(_oneCipherByte);
                        seq.Advance(1);

                        if (decrypted.Length == 0)
                            continue;

                        int need = count - written;
                        int take = Math.Min(need, decrypted.Length);

                        Buffer.BlockCopy(decrypted, 0, destination, offset + written, take);
                        written += take;

                        if (decrypted.Length > take)
                            AppendPending(decrypted, take, decrypted.Length - take);
                    }
                }
                finally
                {
                    _composer.Inbounds.Reader.AdvanceTo(seq.Position, buffer.End);
                }

                if (result.IsCompleted && written < count)
                    throw new EndOfStreamException("Соединение закрыто до получения полного кота");
            }
        }

        private void AppendPending(byte[] source, int sourceOffset, int length)
        {
            if (length == 0) return;

            if (_pendingCount == 0)
            {
                if (_pending.Length < length)
                    _pending = new byte[length];

                Buffer.BlockCopy(source, sourceOffset, _pending, 0, length);
                _pendingOffset = 0;
                _pendingCount = length;
                return;
            }

            int total = _pendingCount + length;

            if (_pending.Length < total)
            {
                var newBuf = new byte[total];
                Buffer.BlockCopy(_pending, _pendingOffset, newBuf, 0, _pendingCount);
                _pending = newBuf;
                _pendingOffset = 0;
            }
            else if (_pendingOffset + _pendingCount + length > _pending.Length)
            {
                Buffer.BlockCopy(_pending, _pendingOffset, _pending, 0, _pendingCount);
                _pendingOffset = 0;
            }

            Buffer.BlockCopy(source, sourceOffset, _pending, _pendingOffset + _pendingCount, length);
            _pendingCount += length;
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                _cts.Cancel();
                await _handleIncoming;
            }
            catch { }
            finally
            {
                _cts.Dispose();
            }
        }
    }
}
