using RabbitMQ.Client;
using System;
using System.Globalization;
using System.Text;

namespace RabbitMQ_Demo.RabbitmqService.ExcDirect
{
    public class D_Producer
    {
        private ConnectionFactory _connectionFac;
        private IConnection _connection;
        private IModel _channel;
        private IBasicProperties _basicProperties;
        private readonly string _excName;
        private readonly string _qName;
        private readonly string _routingKey;

        public D_Producer(string excName, string qName, string routingKey)
        {
            _excName = excName;
            _qName = qName;
            _routingKey = routingKey;
            CreateConnection();
        }

        public ConnectionFactory CreateConnection()
        {
            try
            {
                _connectionFac = new ConnectionFactory
                {
                    HostName = Properties.Settings.Default.RabbitMQ_HostName,
                    UserName = Properties.Settings.Default.RabbitMQ_Username,
                    Password = Properties.Settings.Default.RabbitMQ_Password,
                    Port = Properties.Settings.Default.RabbitMQ_Port,
                };
                _connection = _connectionFac.CreateConnection();
                _channel = _connection.CreateModel();

                _channel.ExchangeDeclare(_excName, ExchangeType.Direct, true);
                _channel.QueueDeclare(_qName, true, false, false, null);
                _channel.QueueBind(_qName, _excName, _routingKey);

                _basicProperties = _channel.CreateBasicProperties();
                _basicProperties.Persistent = true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(DateTime.Now.ToString(CultureInfo.InvariantCulture) + " " + ex.Message + " - " + ex.InnerException + " - " + ex.StackTrace);
            }
            return _connectionFac;
        }

        public bool Send(string message)
        {
            try
            {
                if (_channel == null || _channel.IsClosed)
                {
                    Console.WriteLine("Cannot create connection to RabbitMQ server");
                    return false;
                }

                if (!string.IsNullOrWhiteSpace(message))
                {
                    var payload = Encoding.UTF8.GetBytes(message);
                    if (payload.Length > 0)
                    {
                        if (_channel.IsOpen)
                        {
                            _channel.BasicPublish(_excName, _routingKey, _basicProperties, payload);
                            Console.WriteLine($"Sent a message success to RabbitMQ server = { _connection.Endpoint.HostName}, Exchange = {_excName}, Queue = {_qName}, RoutingKey = {_routingKey}");
                            return true;
                        }
                    }
                }
                return false;
            }
            catch (Exception ex)
            {
                Console.WriteLine(DateTime.Now.ToString(CultureInfo.InvariantCulture) + " " + ex.Message + " - " + ex.InnerException + " - " + ex.StackTrace);
                return false;
            }
        }
    }
}