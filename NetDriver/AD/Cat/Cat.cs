using System;
using AVcontrol;

namespace NetDriver.AD
{
    internal class Cat
    {
        public Head head { get; private set; }
        public Body body { get; private set; }

        public Cat(byte[] bytes)
        {
            head = new Head(bytes[..21]);
            if (bytes.Length > 21) body = new Body(head, bytes[21..]);
        }

        public Cat(Head head, Body body)
        {
            this.head = head;
            this.body = body;
        }

        public Cat(Guid uid, UInt32 contentSize, Cat.Type type, UInt16? packNumInFlow, byte[] content)
        {
            head = new Head(uid, contentSize, type);
            body = new Body(packNumInFlow, content);
        }

        public byte[] Compress()
        {
            if (head == null || body == null)
                return [0];
            var headBytes = Head.Comporess(head);
            var bodyBytes = Body.Compress(body);

            var res = new byte[headBytes.Length + bodyBytes.Length];

            Buffer.BlockCopy(headBytes, 0, res, 0, headBytes.Length);
            Buffer.BlockCopy(bodyBytes, 0, res, headBytes.Length, bodyBytes.Length);

            return res;
        }

        public void InitBody(byte[] content)
        {
            if (head != null && body == null)
            {
                body = new Body(head, content);
            }
        }

        public class Head
        {
            public readonly Guid uid;
            public readonly UInt32 contentSize;
            public readonly Cat.Type type;

            public Head(byte[] content)
            {
                if (content.Length != 21)
                {
                    throw new Exception("Wrong Size");
                }

                uid = new Guid(content.AsSpan(0, 16));
                contentSize = FromBinary.LittleEndian<UInt32>(content.AsSpan(16, 4));
                type = (Cat.Type)content[20];
            }

            public Head(Guid uid, UInt32 contentSize, Cat.Type type)
            {
                this.uid = uid;
                this.contentSize = contentSize;
                this.type = type;
            }

            public static byte[] Comporess(Head head)
            {
                var res = new byte[21];

                Buffer.BlockCopy(head.uid.ToByteArray(), 0, res, 0, 16);
                Buffer.BlockCopy(ToBinary.LittleEndian(head.contentSize), 0, res, 16, 4);

                res[20] = (byte)head.type;

                return res;
            }
        }

        public class Body
        {
            public readonly UInt16? packNumInFlow;
            public readonly byte[] content;

            public Body(Head head, byte[] content)
            {
                if (head.type == Cat.Type.FilePart)
                {
                    packNumInFlow = FromBinary.BigEndian<UInt16>(content[..2]);
                    this.content = content[2..];
                }
                else
                {
                    this.content = content;
                }
            }

            public Body(UInt16? packNumInFlow, byte[] content)
            {
                this.packNumInFlow = packNumInFlow;
                this.content = content;
            }

            public static byte[] Compress(Body body)
            {
                byte[] buff = new byte[(body.packNumInFlow == null ? 0 : 2) + body.content.Length];

                if (body.packNumInFlow != null)
                {
                    Buffer.BlockCopy(ToBinary.BigEndian<UInt16>(body.packNumInFlow.Value), 0, buff, 0, 2);
                    Buffer.BlockCopy(body.content, 0, buff, 2, body.content.Length);
                }
                else
                {
                    Buffer.BlockCopy(body.content, 0, buff, 0, body.content.Length);
                }

                return buff;
            }
        }

        public enum Type : byte
        {
            PING = 0,
            PONG = 1,
            Single = 2,
            Request = 3,
            Answer = 4,
            FileStreamConfig = 4,
            FilePart = 6,
        }
    }
}
