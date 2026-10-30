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
                    var headBytes = new byte[21];
                    // достань из трубы декриптора чуть позже
                    var head = new Cat.Head(headBytes);

                    var bodyBytes = new byte[head.contentSize];
                    // достань из трубы декриптора чуть позже
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
