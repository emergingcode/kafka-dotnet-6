namespace KafkaConsumer.Worker
{
    using DBridge.Messaging.KafkaConsumer;

    using System.Threading;
    using System.Threading.Tasks;

    public class EventProcessor : BackgroundService
    {
        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var tasks = new List<Task>();
            var cancellationToken = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cancellationToken.Cancel();
            };

            var consumer1 =
                        new KafkaConsumer<string, string>(
                            $"consumer-1",
                            "group-A",
                            "localhost:29092,localhost:39093",
                            "demo-topico-1.0")
                        {
                        };

            var consumer2 =
                        new KafkaConsumer<string, string>(
                            $"consumer-2",
                            "group-A",
                            "localhost:29092,localhost:39093",
                            "demo-topico-1.0")
                        {
                        };

            tasks.Add(Task.Run(() => consumer1.Consume(cancellationToken)));
            tasks.Add(Task.Run(() => consumer2.Consume(cancellationToken)));

            Task t = Task.WhenAll(tasks);

            try
            {
                t.Wait();
            }
            catch { }

            if (t.Status == TaskStatus.RanToCompletion)
                Console.WriteLine("All consumers ran succeeded.");
            else if (t.Status == TaskStatus.Faulted)
                Console.WriteLine("Consumer(s) attempts failed.");

            return Task.CompletedTask;
        }
    }
}
