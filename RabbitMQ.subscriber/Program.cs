using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;

namespace RabbitMQ.subscriber
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory();
            factory.Uri = new Uri("amqps://nxdranwu:n_3nr-xZlXx0NoCWuFP05gTqZfp7_hwK@sparrow.rmq.cloudamqp.com/nxdranwu ");

            using var connection = factory.CreateConnection();

            var channel = connection.CreateModel();

            var randomQueueName = channel.QueueDeclare().QueueName; //rastgele kuyruk ismi veriyoruz çünkü her bir consumer farklı bir kuyrukta iletişim halinde olmalı.
            //oluşan bu kuyruğu "logs-fanout" isimli exchange'ime bind etmeliyim.
            channel.QueueBind(randomQueueName, "logs-fanout", null);

            channel.BasicQos(0, 1, false);

            var consumer = new EventingBasicConsumer(channel);

            channel.BasicConsume(randomQueueName, false, consumer);

            Console.WriteLine("loglar dinleniyor");

            consumer.Received += (object sender, BasicDeliverEventArgs e) =>
            {
                var message = Encoding.UTF8.GetString(e.Body.ToArray());

                Thread.Sleep(1500);

                Console.WriteLine("Gelen Mesaj: " + message);

                channel.BasicAck(e.DeliveryTag, false);
            };

            Console.ReadLine();

        }

    }
}
