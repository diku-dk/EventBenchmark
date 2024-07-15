# Online Marketplace - Dapr Benchmark Driver

Examples of input:

%2FUsers%2F<user>%2Fworkspace%2Fbenchmark%2FEventBenchmark%2FConfiguration%2Fdapr_local.json

%2Fwork%2FHome%2FEventBenchmark%2FConfiguration%2Fdapr_local.json

To run:

# Inside the folder
dapr run --app-port 8081 --app-id driver --app-protocol http --dapr-http-port 8082 -- dotnet run --project Dapr.csproj

# Run from the root folder
dapr run --app-port 8081 --app-id driver --app-protocol http --dapr-http-port 8082 -- dotnet run --project Dapr/Dapr.csproj

# Redis Insight

To connect the Redis instance running on Docker in Redis Insight, use the following as host:

docker.for.mac.localhost
