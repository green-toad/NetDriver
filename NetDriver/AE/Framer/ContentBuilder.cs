using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Channels;

namespace NetDriver.AE
{
    internal class ContentBuilder
    {
        public static readonly string swapDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Downloads");
        private readonly FileStream _fs;
        private readonly int _chunkSize;
        private readonly int _chunkCount;

        private readonly ConcurrentDictionary<int, byte?> hash = new();
        private int writeNow = 0;

        private readonly Task _write;
        private readonly Channel<ChunkWriteRQ> writeQueue = Channel.CreateUnbounded<ChunkWriteRQ>();

        private readonly CancellationTokenSource _cts;

        public ContentBuilder(string fileName, int chunkSize, int chunkCount, Func<Guid, Task> disposeBuilder, Guid suid)
        {
            var pathToFile = Path.Combine(swapDir, fileName);
            _chunkSize = chunkSize;
            _chunkCount = chunkCount;

            Directory.CreateDirectory(swapDir);
            _fs = new FileStream(pathToFile, FileMode.Create, FileAccess.Write, FileShare.None, 4096, true);

            _cts = new CancellationTokenSource(TimeSpan.FromSeconds(3000));

            _write = WriterTask(_cts.Token).ContinueWith(async _ =>
            {
                await disposeBuilder(suid);
            }).Unwrap();
        }

        public async Task WriteNewPack(byte[] content, int chunkNumb)
        {
            if (!hash.TryAdd(chunkNumb, null)) return;
            await writeQueue.Writer.WriteAsync(new ChunkWriteRQ(content, chunkNumb));
        }

        private async Task<bool> AddChunk(byte[] content, int chunkNumb)
        {
            long position = (long)chunkNumb * _chunkSize;

            _fs.Position = position;

            await _fs.WriteAsync(content.AsMemory(16, content.Length - 16));

            return Interlocked.Increment(ref writeNow) == _chunkCount;
        }

        private async Task WriterTask(CancellationToken ct)
        {
            var reader = writeQueue.Reader;

            await foreach (var chunk in reader.ReadAllAsync(ct))
            {
                try
                {
                    if (chunk.Content.Length <= 16) continue;
                    if (await AddChunk(chunk.Content, chunk.ChunkNumb)) break;
                }
                catch (OperationCanceledException)
                {

                }
                catch (Exception e)
                {
                    Console.WriteLine($"нужно сделать логи но чуть позже ({e})\n\n");
                }
            }
        }

        private struct ChunkWriteRQ(byte[] cnt, int numb)
        {
            public readonly byte[] Content = cnt;
            public readonly int ChunkNumb = numb;
        }
        
        public async Task DisposeAsync()
        {
            await _cts.CancelAsync();
            writeQueue.Writer.TryComplete();
            await _write;

            await _fs.DisposeAsync();
            _cts.Dispose();
        }
    }
}