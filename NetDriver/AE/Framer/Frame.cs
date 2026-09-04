using System;

namespace NetDriver.AE
{
    internal struct netframe(netframe.Header h, netframe.Content c)
    {
        public readonly Header header = h;
        public readonly Content content = c;
        public struct Header(UInt32 cs, Type ty, UInt32 num = 0)
        {
            public readonly UInt32 contentSize = cs;
            public readonly Type type = ty;
            public readonly UInt32 numInFlow = num;
        }

        public struct Content(Guid fg, byte[] c)
        {
            public readonly Guid frameuid = fg;

            public readonly byte[] content = c;
        }

        public enum Type : byte
        {
            single = 0,
            callbackFrom = 1, // надо ответитрь
            callbackInto = 2, // есть сам ответ
            configurateFlow = 3,
            flowPart = 4,
            PING = 5, // системный мусор
        }
    }
}