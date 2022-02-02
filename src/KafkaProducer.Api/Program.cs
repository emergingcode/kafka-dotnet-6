using DBridge.Messaging.KafkaProducer;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.MapPost("/produce/message", async (int count, string message) =>
{
    for (int i = 0; i < count; i++)
    {
        using (var producer = new KafkaProducer<string, string>(
                                "demo-topico-1.0",
                                "localhost:29092,localhost:39093"))
        {
            await producer.ProduceMessageAsync($"REGISTERING message {message} for a cliente {i} of {count}", $"clientId-{i}");

            if (i % 5 == 0)
            {
                await producer.ProduceMessageAsync($"THE ADDRESS FROM cliente {i} WAS UPDATED", $"clientId-{i}");
            }

        }
    }

    return Results.Ok(message);
});

app.Run();
