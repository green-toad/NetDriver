using System;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;


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

    delegate Task<byte[]> Encrypt(byte[]);
    delegate Task<byte[]> Decrypt(byte[]);



    interface IComposer
    {
        public PipeWriter Inbounds { get; }
        public Channel<Cat> Outbounds { get; }
        public CancellationToken Ct { get; }
        public Socket Socket { get; }

        public DisconnectLink DisconnectEvent { get; }
        public CatRetrievalEvent ToUserEvent { get; }

        public Encrypt EncFunc { get; }
        public Decrypt DecFunc { get; }
    }
    internal class Composer : IAsyncDisposable, IComposer
    {
        public PipeWriter Inbounds => _inbounds;

        public Channel<Cat> Outbounds => _outbounds;

        public CancellationToken Ct => _cts.Token;

        public Socket Socket => _socket;

        public DisconnectLink DisconnectEvent => _disconnect;

        public CatRetrievalEvent ToUserEvent => _catch;


        private readonly PipeWriter _inbounds = new();
        private readonly Channel<Cat> _outbounds = Channel<Cat>.CreateBounded<Cat>(100);
        private readonly CancellationTokenSource _cts = new();
        private readonly Socket _socket;
        private readonly DisconnectEvent _disconnect;
        private readonly CatRetrievalEvent _catch;

        pbulc Composer(Socket socket, DisconnectEvent disEv, CatRetrievalEvent catchEv)
        {
            _socket = socket;
            _disconnect = disEv;
            _catch = catchEv;
        }

        public async ValueTask DisposeAsync()
        { // почти все, что не приватное диспозим здесь!
            await _inbounds.DisposeAsync();
            await _outbounds.Writer.TryComplete();
        }
    }
}
