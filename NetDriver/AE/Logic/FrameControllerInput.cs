using AVcontrol;
using NetDriver.AE;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Channels;

namespace NetDriver.AE
{
    internal class FrameControllerInput : IDisposable
    {
        public readonly Channel<netframe> simpleleOutput = Channel.CreateUnbounded<netframe>();
        public readonly Channel<netframe> answersOnReq = Channel.CreateUnbounded<netframe>();

        public readonly Channel<netframe> SystemSend = Channel.CreateUnbounded<netframe>();

        private readonly ConcurrentDictionary<Guid, ContentBuilder> builderList = new();

        public async Task Distribute(netframe frame)
        {
            switch (frame.header.type)
            {
                case netframe.Type.single:
                    await simpleleOutput.Writer.WriteAsync(frame);
                    break;

                case netframe.Type.callbackInto:
                    await answersOnReq.Writer.WriteAsync(frame);
                    break;

                case netframe.Type.callbackFrom:
                    await simpleleOutput.Writer.WriteAsync(frame);
                    break;

                case netframe.Type.configurateFlow:

                    var suidC = new Guid(frame.content.content.AsSpan(0, 16));
                    int singlePackSize = FromBinary.LittleEndian<int>(frame.content.content.AsSpan(16, 4));
                    int packCount = FromBinary.LittleEndian<int>(frame.content.content.AsSpan(16 + 4, 4));
                    string name = FromBinary.Utf16(frame.content.content.AsSpan(16 + 4 + 4, frame.content.content.Length - (16 + 4 + 4)));

                    builderList.TryAdd(suidC, new ContentBuilder(name, singlePackSize, packCount, DisposeBuilder, suidC));
                    await SystemSend.Writer.WriteAsync(FrameParser.BuildFrame(netframe.Type.callbackInto, frame.content.frameuid, ToBinary.Utf8("yes")));

                    break;

                case netframe.Type.flowPart:

                    var suidP = new Guid(frame.content.content.AsSpan(0, 16));

                    if (builderList.TryGetValue(suidP, out var cb))
                        await cb.WriteNewPack(frame.content.content, (int)frame.header.numInFlow);
                    await SystemSend.Writer.WriteAsync(FrameParser.BuildFrame(netframe.Type.callbackInto, frame.content.frameuid, ToBinary.Utf8("yes")));

                    break;
            }
        }

        private async Task DisposeBuilder(Guid uid)
        {
            if (builderList.Remove(uid, out var res))
                await res.DisposeAsync();
        }

        public void Dispose()
        {
            simpleleOutput.Writer.TryComplete();
            answersOnReq.Writer.TryComplete();
            SystemSend.Writer.TryComplete();
        }
    }
}
