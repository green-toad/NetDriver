using System.ComponentModel.Design;

namespace NetDriver.AE
{
    internal class LiveChecker : IAsyncDisposable
    {
        private readonly CancellationTokenSource _cts = new();
        private readonly FrameControllerOutput _outputControl;
        private readonly Task _pulse;
        private readonly UInt16 _timeout;
        private readonly Action _disconnect;

        public LiveChecker(Action discon, FrameControllerOutput outp, UInt16 timeout)
        {
            _timeout = timeout;
            _disconnect = discon;
            _outputControl = outp;

            _pulse = Task.Run(Checker);
        }

        private async Task Checker()
        {
            UInt16 counter = 0;
            while(!_cts.IsCancellationRequested)
            {
                await Task.Delay(1000 * 25);
                Console.Write("пингую сторону\n");
                var res = await _outputControl.SendWithCallback(FrameParser.BuildFrame(netframe.Type.PING, Guid.NewGuid(), [1]));
                if (res == null)
                {
                    counter++;
                    Console.Write("смерть\n");
                    continue;
                }
                else if (res.Value.content.content[0] != 0)
                {
                    counter++;
                    Console.Write("смерть\n");
                    continue;
                }
                else
                {
                    counter = 0;
                    Console.Write("он жив =)\n");
                }

                if (counter >= _timeout)
                {
                    _disconnect();
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            _cts.Cancel();

            await _pulse;

            _cts.Dispose();
        }
    }
}