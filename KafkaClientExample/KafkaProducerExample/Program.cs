using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using System;

namespace KafkaProducerExample
{
    class Program
    {
        static void Main(string[] args)
        {
            var kafkaOptions = new KafkaOptions(new Uri("http://192.168.1.35:9092"));
            var brokerRouter = new BrokerRouter(kafkaOptions);
            var producer = new TestProducer(brokerRouter);

            Console.WriteLine("Send a Message to TestTopic:");
            while (true)
            {
                producer.SendMessageAsync("TestTopic", Console.ReadLine());
            }
        }
    }

    public class TestProducer
    {
        private readonly IBrokerRouter _brokerRouter;

        public TestProducer(IBrokerRouter brokerRouter)
        {
            _brokerRouter = brokerRouter;
        }

        public void SendMessageAsync(string topic, string message)
        {
            var producer = new Producer(_brokerRouter);

            producer.SendMessageAsync(topic, new[] { new Message(message), }).Wait();
        }
    }
}