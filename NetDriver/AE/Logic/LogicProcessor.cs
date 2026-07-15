using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;

namespace NetDriver.AE
{
    public delegate Task IncomingEvent(ResultContent content); 
    internal class LogicProcessor : IAsyncDisposable
    {
        private IncomingEvent _incomingEvent;

        public readonly FrameControllerOutput output = new();
        public bool alive { get => _incoming.isOpen; }
        private readonly FrameControllerInput _input = new();

        private readonly IncomingController _incoming;
        private readonly OutcomingController _outcoming;

        private readonly CancellationTokenSource _cts = new();
        private readonly Socket _socket;

        private Task A;
        private Task B;
        private Task C;
        private Task D;
        private Task E;
        public LogicProcessor(IncomingEvent ievent, Socket sock)
        {
            _socket = sock;
            _incomingEvent = ievent;

            _incoming = new(sock);
            _outcoming = new(sock);

            A = Task.Run(ExecutorA);
            B = Task.Run(ExecutorB);
            C = Task.Run(ExecutorC);
            D = Task.Run(ExecutorD);
            E = Task.Run(ExecutorE);
        }

        private async Task ExecutorA()
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);

            try
            {
                await foreach (var sf in _input.simpleleOutput.Reader.ReadAllAsync(cts.Token))
                {
                    await _incomingEvent.Invoke(new ResultContent((ResultContent.Type)sf.header.type, sf.content.content, _socket, sf.content.frameuid));
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        private async Task ExecutorB()
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);

            try 
            { 
                await foreach (var sf in _input.answersOnReq.Reader.ReadAllAsync(cts.Token))
                {
                    output.CatchAnswer(sf);
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        private async Task ExecutorC()
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);

            try
            {

                await foreach (var sf in _input.SystemSend.Reader.ReadAllAsync(cts.Token))
                {
                    await output.SendSingle(sf);
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        private async Task ExecutorD()
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);

            try
            {
                await foreach (var sf in output.outcomingStack.Reader.ReadAllAsync(cts.Token))
                {
                    await _outcoming.Send(sf);
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        private async Task ExecutorE()
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);

            while (!cts.IsCancellationRequested)
            {
                var h = await _incoming.GetChunk(9);
                if (h.Length == 0) continue;
                var header = FrameParser.UnpackHeader(h);

                var c = await _incoming.GetChunk(header.contentSize);
                if (c.Length == 0) continue;
                var content = FrameParser.UnpackContent(c);

                await _input.Distribute(new netframe(header, content));
            }
        }

        public async ValueTask DisposeAsync()
        {
            _cts.Cancel();

            await _outcoming.DisposeAsync();
            await _incoming.DisposeAsync();
            output.Dispose();
            _input.Dispose();

            await A;
            await B;
            await C;
            await D;
            await E;

            _cts.Dispose();
        }
    }

    public class ResultContent(ResultContent.Type type, byte[] content, Socket socket, Guid? uid=null)
    {
        public readonly Socket socket = socket;
        public readonly Type type = type;

        public readonly byte[] content = content;

        public readonly Guid? frameuid = uid;

        public enum Type : byte
        {
            single = 0,
            from = 1,
            into = 2,
        }
    }

    public enum FileParametrs
    {
        Straight,
        Random,
        Reverse,
    }
}
