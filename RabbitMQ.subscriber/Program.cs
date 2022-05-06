using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;

namespace RabbitMQ.subscriber
{
    class Program
    { //Producer (publisher) rabbitMQ'ya bir mesaj gönderdiğinde exchange tipi fanout olan exchange'e mesajı gönderir. 
        //Fanout exchange ise herhangi bir filtreleme yapmadan kendisine bağlı olan tüm kuyruklara mesajı iletir.
        //Yani üretilen bir mesaj tüm kuyruklara ayrı ayrı yollanır.
        //örnek olarak rest servis olmadan hava tahmin durumu. producer saat başı hava tahmin durumunu alıyor ve bunu fanout exchange'e gönderiyor. eğer kuyruk yoksa
        //          tüm mesajlar boşa gidiyor. Bu senaryoda fanout exchange'e kuyruk consumer tarafında oluşturulmalı çünkü kaç tane consumer'ın bağlanacağını bilmiyoruz.
        //          Fanout exchange tipinde genellikle kuyrukları consumer'lar oluşturur.
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory();
            factory.Uri = new Uri("amqps://nxdranwu:n_3nr-xZlXx0NoCWuFP05gTqZfp7_hwK@sparrow.rmq.cloudamqp.com/nxdranwu ");

            using var connection = factory.CreateConnection();

            var channel = connection.CreateModel();

            var randomQueueName = channel.QueueDeclare().QueueName; /*"log-database-save-queue"; //random olmasını istemiyoruz çünkü kuyruğun kalıcı olmasını istiyoruz*/
            //rastgele kuyruk ismi veriyoruz çünkü her bir consumer farklı bir kuyrukta iletişim halinde olmalı.

            //channel.QueueDeclare(randomQueueName, true, false, false);

            //oluşan bu kuyruğu "logs-fanout" isimli exchange'ime bind etmeliyim.
            channel.QueueBind(randomQueueName, "logs-fanout", "", null);

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
