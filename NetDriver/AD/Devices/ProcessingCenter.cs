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
                    var headBytes = await ReadBuffer(21);

                    var head = new Cat.Head(headBytes);

                    var bodyBytes = await ReadBuffer(head.contentSize);

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
            catch (Exception e)
            {
                _composer.Loger.Log(this, e);
            }
        }

        private async Task<byte[]> ReadBuffer(UInt32 buffLenght)
        {
            var bytes = new byte[buffLenght];
            while (true)
            {
                var res = await _composer.Decryptor.Reader.ReadAsync(_cts.Token);
                if (res.Buffer.Length < buffLenght)
                {
                    _composer.Decryptor.Reader.AdvanceTo(res.Buffer.Start, res.Buffer.End);
                    continue;
                }
                res.Buffer.Slice(0, buffLenght).CopyTo(bytes);
                _composer.Decryptor.Reader.AdvanceTo(res.Buffer.GetPosition(buffLenght));
                break;
            }
            return bytes;
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
