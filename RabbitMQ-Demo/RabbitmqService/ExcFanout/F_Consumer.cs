using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Globalization;
using System.Text;

namespace RabbitMQ_Demo.RabbitmqService.ExcFanout
{
    public class F_Consumer
    {
        private readonly string _excName;
        private readonly string _qName;

        public F_Consumer(string excName, string qName)
        {
            _excName = excName;
            _qName = qName;
            try
            {
                var connectionFac = new ConnectionFactory
                {
                    HostName = Properties.Settings.Default.RabbitMQ_HostName,
                    UserName = Properties.Settings.Default.RabbitMQ_Username,
                    Password = Properties.Settings.Default.RabbitMQ_Password,
                    Port = Properties.Settings.Default.RabbitMQ_Port,
                    RequestedHeartbeat = 30,
                    AutomaticRecoveryEnabled = true,
                    NetworkRecoveryInterval = TimeSpan.FromSeconds(10),
                    TopologyRecoveryEnabled = true,
                    HandshakeContinuationTimeout = TimeSpan.FromSeconds(30),
                    RequestedConnectionTimeout = 2000,
                };
                using (var connection = connectionFac.CreateConnection())
                {
                    using (var channel = connection.CreateModel())
                    {
                        if (channel.IsOpen)
                        {
                            Console.WriteLine("Ready to receiver messages!");
                            channel.ExchangeDeclare(_excName, ExchangeType.Fanout, true);
                            channel.QueueDeclare(_qName, true, false, false, null);
                            channel.QueueBind(_qName, _excName, "");

                            //handle 1 messages simultaneously
                            channel.BasicQos(0, 1, false);

                            var consumer = new EventingBasicConsumer(channel);
                            if (consumer.Model.IsOpen)
                            {
                                consumer.Received += (model, ea) =>
                                {
                                    var body = ea.Body;
                                    var message = Encoding.UTF8.GetString(body);
                                    Console.WriteLine($"[x] Received a message from RabbitMQ server = {connection.Endpoint.HostName}, Exchange = {ea.Exchange}, Queue = {_qName}, RoutingKey = {ea.RoutingKey}");

                                    //code something...

                                    //acknowledge receipt of the message --> confirm consume message was done
                                    Console.WriteLine("Message: " + message);
                                    channel.BasicAck(ea.DeliveryTag, false);
                                };
                                channel.BasicConsume(_qName, true, consumer);
                            }
                        }
                    };
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine(DateTime.Now.ToString(CultureInfo.InvariantCulture) + " " + ex.Message + " - " + ex.InnerException + " - " + ex.StackTrace);
            }
        }
    }
}