using System;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;
using NetDriver.AD.Devices;


/*
всего 5 элементов

входящий и отправляющий девайсы
композитор
устройство дешифровщик
и обработчик


композитор хранит все ресы

входящий и исходящий слушает и отправляет

между входящим и обработчиком есть прослойка -- дешифрует трафик не аланизируя его

обработчик обробатывает и перекладывает из очереди в очередь
*/
namespace NetDriver.AD
{
    // предпологается, что ни в одном из делегатов либо нет IO операций, либо длительность выполнения их стремится к 0 и ожибать их с await не нужно.
    // тоесть, Channel<***>.CreateBounded<***>(20) . . . Chan.AddAsync() <-- вот так делать нельзя (в нашем частном случае)
    // а вот Channel<***>.CreateUnbounded<***>() . . . Chan.AddAsync() <---- можно, потому что операция по сути происходит мгновенно, и ожидать операции незачем.
    // (^‿^)


    interface IComposer : IAsyncDisposable
    {
        public Pipe Inbounds { get; }
        public Pipe Decryptor { get; }
        public Channel<Cat> Outbounds { get; }

        public ConcurrentDictionary<Guid, TaskCompletionSource<Cat>> AnswerRequests { get; }

        public CancellationToken Ct { get; }
        public Socket Socket { get; }

        public DisconnectLink DisconnectEvent { get; }
        public CatRetrievalEvent ToUserEvent { get; }

        public Encrypt EncFunc { get; }
        public Decrypt DecFunc { get; }

        public LogerDevice Loger { get; }
    }
    internal class Composer : IComposer
    {
        public Pipe Inbounds => _inbounds;

        public Pipe Decryptor => _decryptor;

        public Channel<Cat> Outbounds => _outbounds;

        public CancellationToken Ct => _cts.Token;

        public Socket Socket => _socket;

        public DisconnectLink DisconnectEvent => _disconnect;

        public CatRetrievalEvent ToUserEvent => _catch;

        public Encrypt EncFunc => _enc;

        public Decrypt DecFunc => _dec;

        public ConcurrentDictionary<Guid, TaskCompletionSource<Cat>> AnswerRequests => _answerRequests;

        public LogerDevice Loger => _log;

        private readonly Pipe _inbounds;
        private readonly Pipe _decryptor;
        private readonly Channel<Cat> _outbounds = Channel.CreateBounded<Cat>(100);
        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<Cat>> _answerRequests = new();
        private readonly CancellationTokenSource _cts = new();
        private readonly Socket _socket;
        private readonly DisconnectLink _disconnect;
        private readonly CatRetrievalEvent _catch;
        private readonly Encrypt _enc;
        private readonly Decrypt _dec;

        private readonly LogerDevice _log;
        private readonly InboundDevice _inDev;
        private readonly OutboundDevice _outDev;
        private readonly ProcessingCenterDevice _porcessingCenter;

        private async Task<byte[]> EncPlug(byte[] content) { return content; }
        private async Task<byte[]> DecPlug(byte[] content) { return content; }

        public Composer(
            Socket socket,
            DisconnectLink disEv,
            CatRetrievalEvent catchEv,
            Encrypt? encrypt,
            Decrypt? decrypt
            )
        {
            _socket = socket;
            _disconnect = disEv;
            _catch = catchEv;
            _enc = encrypt == null ? EncPlug : encrypt;
            _dec = decrypt == null ? DecPlug : decrypt;
            _inbounds = new Pipe(new PipeOptions(
                pauseWriterThreshold: 64 * 1024,
                resumeWriterThreshold: 32 * 1024,
                useSynchronizationContext: false));
            _decryptor = new Pipe(new PipeOptions(
                pauseWriterThreshold: 64 * 1024,
                resumeWriterThreshold: 32 * 1024,
                useSynchronizationContext: false));

            _inDev = new(this);
            _outDev = new(this);

            _porcessingCenter = new(this);

            _log = new();
        }

        public async ValueTask DisposeAsync()
        { // почти все, что не приватное диспозим здесь!
            _cts.Cancel();
            await _inbounds.Writer.CompleteAsync();
            await _inbounds.Reader.CompleteAsync();
            await _decryptor.Reader.CompleteAsync();
            await _decryptor.Writer.CompleteAsync();
            await _inDev.DisposeAsync();
            await _outDev.DisposeAsync();
            _outbounds.Writer.Complete();
            _cts.Dispose();
        }
    }
}
