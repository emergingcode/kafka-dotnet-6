namespace DBridge.Messaging.KafkaConsumer
{
    using Confluent.Kafka;

    public class KafkaConsumer<TKeyType, TEventType>
        where TEventType : class
    {
        private readonly string _name;
        private readonly string _group;
        private readonly string _topicName;

        private readonly ConsumerConfig _consumerConfig;

        public KafkaConsumer(
            string name,
            string group,
            string server,
            string topicName)
        {
            _name = name;
            _group = group;
            _topicName = topicName;
            _consumerConfig = new ConsumerConfig
            {
                GroupId = _group,
                BootstrapServers = server,
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                EnablePartitionEof = true,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                ApiVersionRequest = false
            };
        }

        public void Consume(CancellationTokenSource cancellationToken)
        {
            using (var consumer = new ConsumerBuilder<TKeyType, TEventType>(_consumerConfig)
                .SetKeyDeserializer(new JsonDeserializerUTF8<TKeyType>())
                .SetValueDeserializer(new JsonDeserializerUTF8<TEventType>())
                .Build())
            {
                consumer.Subscribe(new[] { _topicName });
                Console.WriteLine($"Initializing Consumer {_name} into consumer group: {_group}");

                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(cancellationToken.Token);

                            if (consumeResult.IsPartitionEOF)
                            {
                                Console.WriteLine(
                                    $"Consumer: {_name} has reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                                continue;
                            }

                            var entityJsonMessage = consumeResult.Message.Value;

                            Console.WriteLine($"Consumer: {_name} has consumed message: '{consumeResult.Message.Value}' from 'Partition: {consumeResult.Partition.Value}, Offset: {consumeResult.TopicPartitionOffset}'.");

                            try
                            {
                                consumer.Commit(consumeResult);
                            }
                            catch (KafkaException e)
                            {
                                Console.WriteLine($"Commit error: {e.Error.Reason}");
                            }
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    consumer.Close();
                }
            }
        }
    }
}