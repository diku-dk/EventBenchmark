var builder = WebApplication.CreateBuilder(args);

// Add services to the container
builder.Services.AddDaprClient();

builder.Services.AddControllers().AddNewtonsoftJson();

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Add functionality to inject IOptions<T>
builder.Services.AddOptions();

builder.Services.AddHttpClient("default").ConfigurePrimaryHttpMessageHandler(() => new HttpClientHandler
{
    UseProxy = false,
    Proxy = null,
    UseCookies = false,
    AllowAutoRedirect = false,
    PreAuthenticate = false,

});

var app = builder.Build();

// Configure the HTTP request pipeline.

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseCloudEvents();

app.MapControllers();

// not needed unless using pub/sub
app.MapSubscribeHandler();

app.Run();