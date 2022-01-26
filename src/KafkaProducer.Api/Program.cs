using DBridge.Messaging.KafkaProducer;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.MapPost("/produce/message", async (int count, string message) =>
{
    //TODO: Topic name "demo-client-v1"

    for (int i = 0; i < count; i++)
    {
        using (var producer = new KafkaProducer<string, string>(
                                "demo-client-v1",
                                "localhost:9092"))
        {
            await producer.ProduceMessageAsync($"REGISTERING a cliente {i} of {count}", $"clientId-{i}"/*customerOrderEvent.PartitionKey()*/);

            if (i % 5 == 0)
            {
                await producer.ProduceMessageAsync($"THE ADDRESS FROM cliente {i} WAS UPDATED", $"clientId-{i}"/*customerOrderEvent.PartitionKey()*/);
            }

        }
    }

    return Results.Ok(message);
});

app.Run();
