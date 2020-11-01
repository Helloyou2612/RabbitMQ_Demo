using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Globalization;
using System.Text;

namespace RabbitMQ_Demo.RabbitmqService.ExcDirect
{
    public class D_Consumer
    {
        private readonly string _excName;
        private readonly string _qName;
        private readonly string _routingKey;

        public D_Consumer(string excName, string qName, string routingKey)
        {
            _excName = excName;
            _qName = qName;
            _routingKey = routingKey;
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
                    RequestedConnectionTimeout = 5000,
                };

                var connection = connectionFac.CreateConnection();
                var channel = connection.CreateModel();
                if (channel.IsOpen)
                {
                    channel.ExchangeDeclare(_excName, ExchangeType.Direct, true);
                    var queueName = channel.QueueDeclare(_qName, true, false, false, null);
                    channel.QueueBind(queueName, _excName, _routingKey);

                    //handle 1 messages simultaneously
                    channel.BasicQos(0, 1, false);
                    var consumer = new EventingBasicConsumer(channel);
                    if (consumer.Model.IsOpen)
                    {
                        consumer.Received += (model, ea) =>
                        {
                            var body = ea.Body;
                            var message = Encoding.UTF8.GetString(body);
                            Console.WriteLine(DateTime.Now.ToString(CultureInfo.InvariantCulture) + " " + _routingKey + " Messages: " + message);

                            //code something...
                            ConsumeRoutingKey(_routingKey, message);


                            Console.WriteLine("Message: " + message);
                            //acknowledge receipt of the message --> confirm consume message was done
                            channel.BasicAck(ea.DeliveryTag, false);
                        };
                        //autoAck: true: automatic acknowledgement mode, false: manually send a proper acknowledgment
                        channel.BasicConsume(queueName, false, consumer);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(DateTime.Now.ToString(CultureInfo.InvariantCulture) + " " + ex.Message + " - " + ex.InnerException + " - " + ex.StackTrace);
            }
        }

        public void ConsumeRoutingKey(string routingKey, string message)
        {
            switch (routingKey)
            {
                case "DirectProducer1":
                    Console.WriteLine($"[x] Received a message RoutingKey = {routingKey}");
                    break;

                case "DirectConsumer2":
                    Console.WriteLine($"[x] Received a message RoutingKey = {routingKey}");
                    break;
            }
        }
    }
}