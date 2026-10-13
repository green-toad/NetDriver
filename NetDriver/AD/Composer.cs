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
    delegate void CatSendEvent(Cat cat);



    interface IComposer
    {
        public PipeWriter Inbounds { get; }
        public Channel<Cat> Outbounds { get; }
        public CancellationToken Ct { get; }
        public Socket Socket { get; }

        public DisconnectLink DisconnectEvent { get; }
        public CatRetrievalEvent ToUserEvent { get; }
        public CatSendEvent FromUserEvent { get; }
    }
    internal class Composer : IAsyncDisposable, IComposer
    {
        public PipeWriter Inbounds => throw new NotImplementedException();

        public Channel<Cat> Outbounds => throw new NotImplementedException();

        public CancellationToken Ct => throw new NotImplementedException();

        public Socket Socket => throw new NotImplementedException();

        public DisconnectLink DisconnectEvent => throw new NotImplementedException();

        public CatRetrievalEvent ToUserEvent => throw new NotImplementedException();

        public CatSendEvent FromUserEvent => throw new NotImplementedException();

        public ValueTask DisposeAsync()
        { // почти все, что не приватное диспозим здесь!
            throw new NotImplementedException();
        }
    }
}
