# EventBenchmark
======================================

```diff
This is an IN-PROGRESS work. Details about how to appropriately configure, deploy, and execute the tool are being added progressively.
```

EventBenchmark is a simulation framework for event-driven microservice applications.
By taking advantage of the [Orleans framework](https://learn.microsoft.com/en-us/dotnet/orleans), EventBenchmark is designed to transparently take advantage of distributed resources and scale, at the same time providing low learning curve, being easy to adapt to different workload characteristics.
Lastly, EventBenchmark makes use of [DuckDB](https://duckdb.org/why_duckdb) to quickly load and query generated data for use during the simulated workloads.

## Target Application
In particular, EventBenchmark is able to model the workload of an online marketplace platform. Further details about the target application can be found in [to be announced].

## Data Set
EventBenchmark relies upon [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) to define the data model and workload for simulation.

## Execution
EventBenchmark requires input configuration in case the user is willing to define a custom workload.
Otherwise the default configuration is assumed:
- Synthetic Data
- X customers
- Y average number of products per seller
- etc
Configuration files. as input. these define the load, ingestion process, and workload characteristics.

## Development 
Run `dotnet run --project Silo` in the project's root directory to initialize the Orleans server. Afterwards, run `dotnet run --project Client` to initialize the Orleans client, which will spawn the workload prescribed. Make sure the target application (including all necessary microservices) are up and running before initializing the benchmark driver (i.e., the Silo and Client projects).