using System;

namespace HTCCosmoGetFgInbound
{
    internal class Program
    {
        static void Main(string[] args)
        {
            while (true)
            {
                try
                {
                    DatabaseService.rfGetFgInbound();
                    DatabaseService.wacGetFgInbound();
                    DatabaseService.sacGetFgInbound();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: {0}", ex.Message.ToString());
                }
                finally
                {
                    Console.WriteLine("System run interval:300000msec");
                    System.Threading.Thread.Sleep(300000);
                }
            }
        }
    }
}
