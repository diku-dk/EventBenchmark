# EventBenchmark
======================================

```diff
- This is an IN-PROGRESS work. Details about how to appropriately configure, deploy, and execute the tool are being added progressively.
```

EventBenchmark is a simulation framework for event-driven microservice applications.
By taking advantage of the [Orleans framework](https://learn.microsoft.com/en-us/dotnet/orleans), EventBenchmark is designed to transparently take advantage of distributed resources, scale, and recover from failures.
At the same time, the framework intends to provide a smooth learning curve, being easy to adapt to different workload scenarios.

## Target Application
In particular, EventBenchmark models the workload of an [online marketplace platform](https://en.wikipedia.org/wiki/Online_marketplace). Experiencing a growing popularity, such platforms offer an e-commerce infrastructure so multiple retailers can offer their products or services to individual consumers.
Further details about the target application can be found in [to be announced].

## Why Event Benchmark?
Given the nature of modern applications that take advantage of asynchronous events as an abstraction for decoupling distributed components, it is pressing that a simulation framework be able to reflect the interactive behavior of these autonomous components.
In this direction, EventBenchmark packages interactive actors that are able to proactively react to events emitted by the application being experimented, thus allowing for a simulation that is more close to reality than traditional benchmark drivers.
EventBenchmark relies on [Apache Kafka](https://github.com/apache/kafka) to consume the events published by the target application and internally makes use of [Orleans Streams](https://learn.microsoft.com/en-us/dotnet/orleans/streaming/) to decouple defined actors.

## Data Set
EventBenchmark relies upon [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) to define the data model and workload for simulation.
EventBenchmark also benefits from [DuckDB](https://duckdb.org/why_duckdb) to quickly load and query generated data for use during the simulated workloads.
[Dapper](https://github.com/DapperLib/Dapper) is used to map rows to objects. [Bogus](https://github.com/bchavez/Bogus) is used to generate faithful synthetic data.

## Execution
EventBenchmark requires input configuration in case the user is willing to define a custom workload.
Otherwise the default configuration is assumed:
- Synthetic Data
- X customers
- Y average number of products per seller
- etc
Configuration files. as input. these define the load, ingestion process, and workload characteristics.

## Development 
Run `dotnet run --project Silo` in the project's root directory to initialize the Orleans server.
Afterwards, run `dotnet run --project Client` to initialize the Orleans client, which will spawn the workload prescribed.
Make sure the target application (including all necessary microservices) are up and running before initializing the benchmark driver (i.e., the Silo and Client projects).

## Useful links
https://stackoverflow.com/questions/44374074/copy-files-to-output-directory-using-csproj-dotnetcore

https://stackoverflow.com/questions/4421633/who-is-listening-on-a-given-tcp-port-on-mac-os-x

## Interesting links
https://stackoverflow.com/questions/41017164/setting-a-specific-grain-to-have-max-n-instances-per-silo
https://github.com/dotnet/orleans/issues/3071
https://sergeybykov.github.io/orleans/Documentation/clusters_and_clients/configuration_guide/shutting_down_orleans.html