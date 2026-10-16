using System;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;
using NetDriver.AD.Devices;


/*
всего 4 элемента

входящий и отправляющий девайсы
композитор
и обработчик


композитор хранит все ресы

входящий и исходящий слушает и отправляет

обработчик обробатывает и перекладывает из очереди в очередь
*/
namespace NetDriver.AD
{
    // предпологается, что ни в одном из делегатов либо нет IO операций, либо длительность выполнения их стремится к 0 и ожибать их с await не нужно.
    // тоесть, Channel<***>.CreateBounded<***>(20) . . . Chan.AddAsync() <-- вот так делать нельзя
    // а вот Channel<***>.CreateUnbounded<***>() . . . Chan.AddAsync() <---- можно, потому что операция по сути происходит мнгновенно, и ожидать операции незачем.
    // (^‿^)
    delegate void DisconnectLink(Socket socket);
    delegate void CatRetrievalEvent(Cat cat);

    delegate Task<byte[]> Encrypt(byte[] data);
    delegate Task<byte[]> Decrypt(byte[] data);



    interface IComposer
    {
        public Pipe Inbounds { get; }
        public Channel<Cat> Outbounds { get; }

        public ConcurrentDictionary<Guid, Cat> AnswerRequests = new();

        public CancellationToken Ct { get; }
        public Socket Socket { get; }

        public DisconnectLink DisconnectEvent { get; }
        public CatRetrievalEvent ToUserEvent { get; }

        public Encrypt EncFunc { get; }
        public Decrypt DecFunc { get; }
    }
    internal class Composer : IAsyncDisposable, IComposer
    {
        public Pipe Inbounds => _inbounds;

        public Channel<Cat> Outbounds => _outbounds;

        public CancellationToken Ct => _cts.Token;

        public Socket Socket => _socket;

        public DisconnectLink DisconnectEvent => _disconnect;

        public CatRetrievalEvent ToUserEvent => _catch;

        public Encrypt EncFunc => _enc;

        public Decrypt DecFunc => _dec;

        private readonly Pipe _inbounds;
        private readonly Channel<Cat> _outbounds = Channel.CreateBounded<Cat>(100);
        private readonly CancellationTokenSource _cts = new();
        private readonly Socket _socket;
        private readonly DisconnectLink _disconnect;
        private readonly CatRetrievalEvent _catch;
        private readonly Encrypt _enc;
        private readonly Decrypt _dec;

        private readonly InboundDevice _inDev;
        private readonly OutboundDevice _outDev;

        public Composer(
            Socket socket,
            DisconnectLink disEv,
            CatRetrievalEvent catchEv,
            Encrypt encrypt,
            Decrypt decrypt
            )
        {
            _socket = socket;
            _disconnect = disEv;
            _catch = catchEv;
            _enc = encrypt;
            _dec = decrypt;
            _inbounds = new Pipe(new PipeOptions(
                pauseWriterThreshold: 64 * 1024,
                resumeWriterThreshold: 32 * 1024,
                useSynchronizationContext: false));

            _inDev = new(this);
            _outDev = new(this);
        }

        public async ValueTask DisposeAsync()
        { // почти все, что не приватное диспозим здесь!
            await _inbounds.Writer.CompleteAsync();
            await _inbounds.Reader.CompleteAsync();
            await _inDev.DisposeAsync();
            await _outDev.DisposeAsync();
            _outbounds.Writer.Complete();
        }
    }
}
