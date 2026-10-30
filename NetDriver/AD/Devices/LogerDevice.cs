using System;

namespace NetDriver.AD.Devices
{
    internal class LogerDevice
    {
        public void Log(Object obj, Exception ex)
        {
            Console.Write($"[{obj.GetType}] {ex.Message}" + "\n");
        }
    }
}
