# Online Marketplace Benchmark Driver
======================================

Online Marketplace is a benchmark modeling an event-driven microservice system in the marketplace application domain. It is design to reflect emerging data management requirements and challenges faced by microservice developers in practice. This project contains the benchmark driver for Online Marketplace. The driver is responsible to manage the lifecycle of an experiment, including data generation, data population, workload submission, and metrics collection.

## Table of Contents
- [Online Marketplace](#marketplace)
    * [Prerequisites](#prerequisites)
    * [Application Domain](#domain)
    * [Required APIs](#apis)
    * [Implementations](#impl)
- [Benchmark Driver](#driver)
    * [Description](#description)
    * [Data Generation](#data)
    * [Configuration](#config)
    * [Running an Experiment](#run)
- [Etc](#etc)

## <a name="marketplace"></a>Online Marketplace Driver

### <a name="prerequisites"></a>Prerequisites

* [.NET Framework 7](https://dotnet.microsoft.com/en-us/download/dotnet/7.0)
* IDE (if you want to modify or debug the code): [Visual Studio](https://visualstudio.microsoft.com/vs/community/) or [VSCode](https://code.visualstudio.com/)
* A multi-core machine with appropriate memory size in case generated data is kept in memory
* Linux- or MacOS-based operating system

### <a name="domain"></a>Application Domain

Online Marketplace models the workload of an [online marketplace platform](https://en.wikipedia.org/wiki/Online_marketplace). Experiencing a growing popularity, such platforms offer an e-commerce technology infrastructure so multiple retailers can offer their products or services to a big consumer base.

### <a name="apis"></a>Required APIs

The driver requires some HTTP APIs to be exposed in order to setup the target data platform prior to workload submission and to be able to actually submit transaction requests.

API                  | HTTP Request Type  | Miroservice    |  Description |
-------------------- |------------------- |--------------- |--------------|
/cart/{customerId}/add | PUT | Cart | Add a product to a customer's cart |
/cart/{customerId}/checkout | POST | Cart | Checkout a cart |
/cart/{customerId}/seal | POST | Cart | Reset a cart |
/customer | POST | Customer | Register a new customer |
/product  | POST  | Product | Register a new product |
/product  | PATCH | Product | Update a product's price |
/product  | PUT   | Product | Replace a product |
/seller   | POST  | Seller | Register a new seller |
/seller/dashboard/{sellerId} | GET | Seller | Retrieve seller's dashboard for a given a seller |
/shipment/{tid} | PATCH | Shipment | Update packages to 'delivered' status | 
/stock | POST | Stock | Register a new stock item |

For the requests that modify microservices' state (POST/PATCH/PUT), refer to classes present in [Common](Common/Entities) to understand the expected payload.

### <a name="impl"></a>Implementations

Right now there are two implementations of Online Marketplace available: [Orleans](https://github.com/diku-dk/MarketplaceOnOrleans) and [Statefun](https://github.com/diku-dk/MarketplaceOnStatefun). In case you want to reproduce experiments, their repositories contain instructions on how to configure and deploy Online Marketplace.

## <a name="driver"></a>Benchmark Driver

### <a name="Description"></a>Description

The driver is written in C# and takes advantage over the thread management facilities provided by .NET framework. It is strongly recommended to analyze the subprojects [Orleans](Orleans) and [Statefun](Statefun) to understand how to extend the driver to run experiments in other data platforms. Further instructions will be included soon.

### <a name="data"></a>Data Generation

The driver uses [DuckDB](https://duckdb.org/why_duckdb) to store and query generated data during the workload submission. The library [DuckDB.NET](https://github.com/Giorgi/DuckDB.NET) is used to bridge .NET with DuckDB. However, only Unix-based operating systems are supported. As the driver depends on the data stored in DuckDB, unfortunately it is not possible to run the benchmark in Windows-based operating systems.

In addition, [Dapper](https://github.com/DapperLib/Dapper) is used to map rows to objects. [Bogus](https://github.com/bchavez/Bogus) is used to generate faithful synthetic data. It is worthy noting that users can also generate data in memory to use in experiments. More info about can be found in [Config](#config)

### <a name="config"></a>Configuration

The driver requires a configuration file to be provided at startup. The configuration prescribes several important aspects of the experiment, including the transaction ratio, the API addresses, the data set parameters, the amount of concurrency, and more. An example configuration, with comments included when the parameter name is not auto-explanable, is shown below.

```
{
    "connectionString": "Data Source=file.db", // defines the data source. if in-memory, set "Data Source=:memory"
    "numCustomers": 100000,
    "numProdPerSeller": 10,
    "qtyPerProduct": 10000,
    "executionTime": 60000, // prescribes each experiment's run total time
    "epoch": 10000, // defines whether the output result will show metrics 
    "delayBetweenRequests": 0,
    "delayBetweenRuns": 0,
    // the transaction ratio
    "transactionDistribution": {
        "CUSTOMER_SESSION": 30,
        "QUERY_DASHBOARD": 35,
        "PRICE_UPDATE": 38,
        "UPDATE_PRODUCT": 40,
        "UPDATE_DELIVERY": 100
    },
    "concurrencyLevel": 48,
    "ingestionConfig": {
        "strategy": "WORKER_PER_CPU",
        "concurrencyLevel": 32,
        // these entries are mandatory
        "mapTableToUrl": {
            "sellers": "http://orleans:8081/seller",
            "customers": "http://orleans:8081/customer",
            "stock_items": "http://orleans:8081/stock",
            "products": "http://orleans:8081/product"
        }
    },
    // it defines the possible multiple runs this experiment contains
    "runs": [
        {
            "numProducts": 100000,
            "sellerDistribution": "UNIFORM",
            "keyDistribution": "UNIFORM"
        }
    ],
    // defines the APIs that should be contact at the end of every run
    "postRunTasks": [
    ],
    // defines the APIs that should be contact at the end of the experiment
    "postExperimentTasks": [
        {
            "name": "cleanup",
            "url": "http://orleans:8081/cleanup"
        }
    ],
    // defines aspects related to customer session
    "customerWorkerConfig": {
        "maxNumberKeysToAddToCart": 10,
        "minMaxQtyRange": {
            "min": 1,
            "max": 10
        },
        "checkoutProbability": 100,
        "voucherProbability": 5,
        "productUrl": "http://orleans:8081/product",
        "cartUrl": "http://orleans:8081/cart",
    },
    "sellerWorkerConfig": {
        // adjust price percentage range
        "adjustRange": {
            "min": 1,
            "max": 10
        },
        "sellerUrl": "http://orleans:8081/seller",
        "productUrl": "http://orleans:8081/product"
    },
    "deliveryWorkerConfig": {
        "shipmentUrl": "http://orleans:8081/shipment"
    }
}

```

Example configuration files are found in [Configuration](Configuration).

### <a name="run"></a>Running an Experiment

Once the configuration is set, and assuming the target data platform is up and running (ready to receive requests), we can initialize the benchmark driver. In this repository root folder, run the following commands for the respective data platforms:

- Orleans
```
dotnet run --project Orleans <configuration file path>
```

- Statefun
```
dotnet run --project Statefun <configuration file path>
```

### <a name="menu"></a>Driver Menu

In both cases, the following menu will be shown to the user:

```
 Select an option:
 1 - Generate Data
 2 - Ingest Data
 3 - Run Experiment
 4 - Full Experiment (i.e., 1, 2, and 3)
 5 - Parse New Configuration
 q - Exit
```

Through the menu, the user can select the benchmark task, including data generation (1), data ingestion into the data platform (2), and (3) workload submission.

## <a name="etc"></a>Etc

## Useful links
- [How to copy files to output directory](https://stackoverflow.com/questions/44374074/copy-files-to-output-directory-using-csproj-dotnetcore)
- [Who is listening to a given port?](https://stackoverflow.com/questions/4421633/who-is-listening-on-a-given-tcp-port-on-mac-os-x)
- [Docker deployment](http://sergeybykov.github.io/orleans/1.5/Documentation/Deployment-and-Operations/Docker-Deployment.html)
- [Interlocked](https://learn.microsoft.com/en-us/dotnet/api/system.threading.interlocked.increment?view=net-7.0&redirectedfrom=MSDN#System_Threading_Interlocked_Increment_System_Int32__)
- [Locust](https://github.com/GoogleCloudPlatform/microservices-demo/blob/main/src/loadgenerator/locustfile.py)