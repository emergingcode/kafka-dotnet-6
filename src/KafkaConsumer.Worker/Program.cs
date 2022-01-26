using KafkaConsumer.Worker;

using IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((hostContext, services) =>
    {
        services.AddHostedService<EventProcessor>();
    })
    .Build();

await host.RunAsync();
