using System;
using System.Net.Sockets;

namespace NetDriver.AD
{
    public delegate void DisconnectLink(Socket socket);
    public delegate void CatRetrievalEvent(Cat cat);

    public delegate Task<byte[]> Encrypt(byte[] data);
    public delegate Task<byte[]> Decrypt(byte[] data);

    public class Networker
    {
        private readonly IComposer _composer;
        public Networker(
            Socket socket,
            DisconnectLink disconEv,
            CatRetrievalEvent catchEv,
            Encrypt? encrypt = null,
            Decrypt? decrypt = null
        )
        {
            _composer = new Composer(
                socket,
                disconEv,
                catchEv,
                encrypt,
                decrypt
            );
        }

        public async Task<Cat?> Send(Cat cat, UInt16 timeout = 2000)
        {
            switch (cat.head.type)
            {
                case Cat.Type.Request:
                    await _composer.Outbounds.Writer.WriteAsync(cat);
                    using (var token = CancellationTokenSource.CreateLinkedTokenSource(_composer.Ct))
                    {
                        token.CancelAfter(timeout);
                        var tcs = new TaskCompletionSource<Cat>(TaskCreationOptions.RunContinuationsAsynchronously);
                        _composer.AnswerRequests.TryAdd(cat.head.uid, tcs);

                        using (token.Token.Register(() =>
                        {
                            if (_composer.AnswerRequests.TryRemove(cat.head.uid, out var tcsLocal))
                                tcsLocal.TrySetCanceled();
                        }))
                        {
                            try
                            {
                                await tcs.Task.WaitAsync(token.Token);
                                return tcs.Task.Result;
                            }
                            catch (OperationCanceledException)
                            {
                                _composer.AnswerRequests.Remove(cat.head.uid, out _);
                                return null;
                            }
                        }
                    }

                default:
                    await _composer.Outbounds.Writer.WriteAsync(cat);
                    return null;
            }
        }
    }
}
