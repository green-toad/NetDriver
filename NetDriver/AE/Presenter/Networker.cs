using System;
using System.Net.Sockets;

namespace NetDriver.AE
{
    public delegate Task<byte[]> EncryptMethod(byte[] content);
    public delegate Task<byte[]> DecryptMethod(byte[] content);
    public class Networker
    {
        private readonly LogicProcessor _logic;
        private readonly Socket _socket;

        public Networker(Socket sock, IncomingEvent ievent, DisconnectEvent devent, EncryptMethod? emethod=null, DecryptMethod? dmethod=null)
        {
            _logic = new(ievent, devent, sock, emethod, dmethod);
            _socket = sock;
        }

        public async Task<ResultContent?> Send(bool withcallback, byte[] content, int timeout=2000)
        {
            if (withcallback)
            {
                var a = await _logic.output.SendWithCallback(FrameParser.BuildFrame(netframe.Type.callbackFrom, Guid.NewGuid(), content), timeout);
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