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

app.MapPost("/produce/message", (int count, string message) =>
{
    //TODO: Iterate loop until count and send message concatenated with count
    //TODO: Topic name "demo-cliente-v1"

    return Results.Ok(message);
});

app.Run();
