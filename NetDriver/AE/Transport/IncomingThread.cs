using System;
using System.Buffers;
using System.IO;
using System.IO.Pipelines;
using System.Linq.Expressions;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace NetDriver.AE
{
    internal class IncomingController : IAsyncDisposable
    {
        private readonly Socket _socket;
        private readonly Task _readingTask;
        private readonly Pipe _pipe = new Pipe();
        private readonly CancellationTokenSource _cts = new();
        private bool _disposed;

        public IncomingController(Socket socket)
        {
            _socket = socket ?? throw new ArgumentNullException(nameof(socket));
            _readingTask = ReadingAsync();
        }
        private async Task ReadingAsync()
        {
            byte[] buffer = ArrayPool<byte>.Shared.Rent(8192);
            try
            {
                while (!_cts.IsCancellationRequested)
                {
                    int received;
                    try
                    {
                        received = await _socket.ReceiveAsync(buffer.AsMemory(0, buffer.Length), _cts.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (SocketException ex)
                    {
                        Console.WriteLine($"[IncomingController] Socket error: {ex.Message}");
                        break;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[IncomingController] Unexpected error: {ex}");
                        break;
                    }

                    if (received == 0)
                    {
                        break;
                    }

                    await _pipe.Writer.WriteAsync(buffer.AsMemory(0, received), _cts.Token);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
                await _pipe.Writer.CompleteAsync();
            }
        }

        public async Task<byte[]> GetChunk(uint chunkSize)
        {
            if (_disposed)
                return new byte[0];

            var result = new byte[chunkSize];
            int totalRead = 0;
            try
            {
                while (totalRead < chunkSize)
                {
                    ReadResult readResult = await _pipe.Reader.ReadAsync(_cts.Token);
                    ReadOnlySequence<byte> buffer = readResult.Buffer;
                    long available = buffer.Length;
                    int needed = (int)chunkSize - totalRead;
                    int toCopy = (int)Math.Min(available, needed);

                    buffer.Slice(0, toCopy).CopyTo(result.AsSpan(totalRead));
                    totalRead += toCopy;

                    _pipe.Reader.AdvanceTo(buffer.Slice(toCopy).Start, buffer.Slice(toCopy).Start);

                    if (readResult.IsCompleted && buffer.Length == 0)
                    {
                        throw new InvalidOperationException("Connection closed before complete chunk was received.");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                return new byte[0];
            }
            return result;
        }
        public async ValueTask DisposeAsync()
        {
            if (_disposed) return;
            _disposed = true;

            _cts.Cancel();

            await _readingTask.ConfigureAwait(false);

            _cts.Dispose();
            _pipe.Writer.Complete();
            _pipe.Reader.Complete();
        }
    }
}