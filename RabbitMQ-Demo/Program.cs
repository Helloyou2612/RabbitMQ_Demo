using RabbitMQ_Demo.RabbitmqService.ExcDirect;
using RabbitMQ_Demo.RabbitmqService.ExcFanout;
using System;

namespace RabbitMQ_Demo
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var showMenu = true;
            while (showMenu)
            {
                showMenu = MainMenu();
            }
        }

        private static bool MainMenu()
        {
            Console.Clear();
            Console.WriteLine("Choose an option:");
            Console.WriteLine("e. Exist");
            Console.WriteLine("1. ExcFanout_SendMessageToQueue");
            Console.WriteLine("2. ExcFanout_ConsumeMessageFromQueue");
            Console.WriteLine("3. ExcDirec_SendMessageToQueue");
            Console.WriteLine("4. ExcDirect_ConsumeMessageFromQueue");
            Console.WriteLine("\r\nSelect an option: ");

            switch (Console.ReadLine())
            {
                case "1":
                    ExcFanout_SendMessageToQueue();
                    return true;

                case "2":
                    ExcFanout_ConsumeMessageFromQueue();
                    return true;

                case "3":
                    ExcDirect_SendMessageToQueue();
                    return true;

                case "4":
                    ExcDirect_ConsumeMessageFromQueue();
                    return true;

                case "e":
                    return false;

                default:
                    return true;
            }
        }

        private static void ExcFanout_SendMessageToQueue()
        {
            var producer = new F_Producer(Properties.Settings.Default.RabbitMQ_Fanout_ExcName, Properties.Settings.Default.RabbitMQ_Fanout_QueueName);
            var i = 0;
            while (i < 10)
            {
                System.Threading.Thread.Sleep(300);
                var message = "Exchange Fanout - Hello World " + i;
                producer.Send(message);
                i++;
            }
            Console.ReadLine();
        }

        private static void ExcFanout_ConsumeMessageFromQueue()
        {
            _ = new F_Consumer(Properties.Settings.Default.RabbitMQ_Fanout_ExcName, Properties.Settings.Default.RabbitMQ_Fanout_QueueName);
            Console.ReadLine();
        }

        private static void ExcDirect_SendMessageToQueue()
        {
            Console.Write("Enter a queue name: ");
            var qName = Console.ReadLine();

            Console.Write("Enter routingKey: ");
            var routingKey = Console.ReadLine();

            var producer = new D_Producer(Properties.Settings.Default.RabbitMQ_Direct_ExcName, qName, routingKey);
            var i = 0;
            while (i <= 10)
            {
                System.Threading.Thread.Sleep(300);
                var message = "Exchange Direct - Hello World " + i;
                producer.Send(message);
                i++;
            }
            Console.ReadLine();
        }

        private static void ExcDirect_ConsumeMessageFromQueue()
        {
            Console.Write("Enter a queue name: ");
            var qName = Console.ReadLine();

            Console.Write("Enter routingKey: ");
            var routingKey = Console.ReadLine();

            _ = new D_Consumer(Properties.Settings.Default.RabbitMQ_Direct_ExcName, qName, routingKey);
            Console.ReadLine();
        }
    }
}