using AVcontrol;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Channels;

namespace NetDriver.AE
{
    internal class FrameControllerOutput : IAsyncDisposable
    {
        private readonly CancellationTokenSource _cts = new();

        public readonly Channel<byte[]> outcomingStack = Channel.CreateUnbounded<byte[]>();

        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<netframe>> waitingCallback = new();

        public async Task SendSingle(netframe message)
        {
            await outcomingStack.Writer.WriteAsync(FrameBuilder.PackFrame(message));
        }

        public async Task<netframe?> SendWithCallback(netframe message, int timeout = 2000)
        {
            await SendSingle(message);

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
            cts.CancelAfter(timeout);

            var tcs = new TaskCompletionSource<netframe>(TaskCreationOptions.RunContinuationsAsynchronously);
            waitingCallback.TryAdd(message.content.frameuid, tcs);

            using (cts.Token.Register(() =>
            {
                if (waitingCallback.TryRemove(message.content.frameuid, out var tcsLocal))
                    tcsLocal.TrySetCanceled();
            }))
            {
                try
                {
                    await tcs.Task.WaitAsync(cts.Token);
                    return tcs.Task.Result;
                }
                catch (OperationCanceledException)
                {
                    return null;
                }
            }
        }

        public bool CatchAnswer(netframe content)
        {
            if (waitingCallback.TryRemove(content.content.frameuid, out var res))
            {
                res.TrySetResult(content);
                return true;
            }
            return false;
        }

        public async Task SendFile(string path, FileParametrs parametr, int part = 1024 * 1024 * 32)
        {
            try
            {
                byte[] fileName = ToBinary.Utf16(Path.GetFileName(path));
                FileInfo fileInfo = new FileInfo(path);

                long fileSize = fileInfo.Length;
                int piceCount = (int)((fileSize + part - 1) / part);

                var fileSuid = Guid.NewGuid();

                //[suid : 16][single pack size : 4][pack num : 4][name : name lenght]
                var mcontent = new byte[16 + 4 + 4 + fileName.Length];

                Buffer.BlockCopy(fileSuid.ToByteArray(), 0, mcontent, 0, 16);
                Buffer.BlockCopy(ToBinary.LittleEndian(part), 0, mcontent, 16, 4);
                Buffer.BlockCopy(ToBinary.LittleEndian(piceCount), 0, mcontent, 16 + 4, 4);
                Buffer.BlockCopy(fileName, 0, mcontent, 16 + 4 + 4, fileName.Length);

                var conf = FrameParser.BuildFrame(netframe.Type.configurateFlow, Guid.NewGuid(), mcontent);

                if (await SendWithCallback(conf) == null) return;

                var sendingData = new Dictionary<Task<netframe?>, int>();
                using (FileStream fs = new FileStream(path, FileMode.Open, FileAccess.Read))
                {

                    int[] partIndices = Enumerable.Range(0, piceCount).ToArray();
                    switch (parametr)
                    {
                        case FileParametrs.Random:
                            Random.Shared.Shuffle(partIndices);
                            break;
                        case FileParametrs.Reverse:
                            Array.Reverse(partIndices);
                            break;
                    }

                    byte[] buffer = ArrayPool<byte>.Shared.Rent(part);

                    foreach (int sn in partIndices)
                    {
                        long offset = sn * part;
                        int bytesToRead = (int)Math.Min(part, fileSize - offset);

                        fs.Seek(offset, SeekOrigin.Begin);
                        int bytesRead = await fs.ReadAsync(buffer, 0, bytesToRead);

                        Console.WriteLine($"send pack №{sn} (random order)");

                        byte[] dataToSend = new byte[bytesRead + 16];
                        Array.Copy(fileSuid.ToByteArray(), 0, dataToSend, 0, 16);
                        Array.Copy(buffer, 0, dataToSend, 16, bytesRead);

                        var msg = FrameParser.BuildFrame(netframe.Type.flowPart, Guid.NewGuid(), dataToSend, (UInt32)sn);
                        sendingData.Add(SendWithCallback(msg, 500), sn);
                    }
                    ArrayPool<byte>.Shared.Return(buffer);
                }

                while (sendingData.Any())
                {
                    var ct = await Task.WhenAny(sendingData.Keys);

                    var res = await ct;

                    if (res == null)
                    {
                        var buffer = new byte[part];
                        sendingData.TryGetValue(ct, out var sn);
                        using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read))
                        {
                            fs.Seek(sn * part, SeekOrigin.Begin);
                            int bytesRead = fs.Read(buffer, 0, part);
                            byte[] dataToSend = new byte[bytesRead + 16];
                            Array.Copy(fileSuid.ToByteArray(), 0, dataToSend, 0, 16);
                            Array.Copy(buffer, 0, dataToSend, 16, part);

                            Console.WriteLine($"re-send pack №{sn}");
                            var msg = FrameParser.BuildFrame(netframe.Type.flowPart, Guid.NewGuid(), dataToSend, (UInt32)sn);
                            sendingData.Add(SendWithCallback(msg, 500), sn);
                        }
                    }
                    sendingData.Remove(ct, out _);
                }
            }
            catch (OperationCanceledException)
            {

            }
            catch (Exception e)
            {
                Console.WriteLine($"нужно сделать логи но чуть позже ({e})\n\n");
            }
        }

        public async ValueTask DisposeAsync()
        {
            _cts.Cancel();
            outcomingStack.Writer.TryComplete();
            _cts.Dispose();
        }
    }
}
