# Online Marketplace - Dapr Benchmark Driver

Example of input:

%2FUsers%2F<user>%2Fworkspace%2Fbenchmark%2FEventBenchmark%2FConfiguration%2Fdapr_local.json

To run:

# Inside the folder
dapr run --app-port 8081 --app-id driver --app-protocol http --dapr-http-port 8082 -- dotnet run --project Dapr.csproj

# Run from the root folder
dapr run --app-port 8081 --app-id driver --app-protocol http --dapr-http-port 8082 -- dotnet run --project Dapr/Dapr.csproj

# Connect Redis Insight to Redis in Docker
docker.for.mac.localhost
