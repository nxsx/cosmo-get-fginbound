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
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: {0}", ex.Message.ToString());
                }
                finally
                {
                    System.Threading.Thread.Sleep(300000);
                }
            }
        }
    }
}
