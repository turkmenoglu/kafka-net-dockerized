using System;
using System.Text;
using KafkaNet;
using KafkaNet.Model;

namespace KafkaConsumerExample
{
    public class TestConsumer
    {
        private readonly IBrokerRouter _brokerRouter;

        public TestConsumer(IBrokerRouter brokerRouter)
        {
            _brokerRouter = brokerRouter;
        }

        public void StartConsume(string topic)
        {
            Console.WriteLine($"Consuming {topic}");

            var consumer = new Consumer(new ConsumerOptions(topic, _brokerRouter));

            foreach (var message in consumer.Consume())
            {
                Console.WriteLine("Response: PartitionId:{0}, Offset:{1} Message:{2}",
                    message.Meta.PartitionId, message.Meta.Offset, Encoding.UTF8.GetString(message.Value));
            }
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var kafkaOptions = new KafkaOptions(new Uri("http://192.168.1.35:9092"));
            var brokerRouter = new BrokerRouter(kafkaOptions);
            var consumer = new TestConsumer(brokerRouter);

            consumer.StartConsume("TestTopic");
        }
    }
}