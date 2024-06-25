# Online Marketplace - Dapr Benchmark Driver

To run:

# Inside the folder
dapr run --app-port 8081 --app-id driver --app-protocol http --dapr-http-port 8082 -- dotnet run --project Dapr.csproj

# Run from the root folder
dapr run --app-port 8081 --app-id driver --app-protocol http --dapr-http-port 8082 -- dotnet run --project Dapr/Dapr.csproj

