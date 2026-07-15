using System;
using System.Net.Sockets;

namespace NetDriver.AE
{
    public class Networker
    {
        public bool alive { get => _logic.alive; }
        private readonly LogicProcessor _logic;
        private readonly Socket _socket;

        public Networker(Socket sock, IncomingEvent ievent)
        {
            _logic = new(ievent, sock);
            _socket = sock;
        }

        public async Task<ResultContent?> Send(bool withcallback, byte[] content, int? timeout=null)
        {
            if (withcallback)
            {
                var a = await _logic.output.SendWithCallback(FrameParser.BuildFrame(netframe.Type.callbackFrom, Guid.NewGuid(), content));
                if (a != null)
                    return new ResultContent((ResultContent.Type)a.Value.header.type, a.Value.content.content, _socket);
                return null;
            }
            else
            {
                await _logic.output.SendSingle(FrameParser.BuildFrame(netframe.Type.single, Guid.NewGuid(), content));
                return null;
            }
        }

        public async Task Answer(byte[] content, Guid suid)
        {
            await _logic.output.SendSingle(FrameParser.BuildFrame(netframe.Type.callbackInto, suid, content));
        }

        public async Task SendFile(string path, FileParametrs param, int part = 1024 * 1024 * 32)
        {
            await _logic.output.SendFile(path, param, part);
        }

        public async Task Dispose()
        {
            await _logic.DisposeAsync();
        }
    }
}