using Common.Experiment;
using Newtonsoft.Json;

namespace Tests.Driver;

public class ParseTest
{

    private static string json = "{\n        \"connectionString\": \"DataSource=:memory:\",\n        \"numCustomers\": 1000,\n        \"numProdPerSeller\": 10,\n        \"executionTime\": 60000,\n        \"epoch\": 10000,\n        \"delayBetweenRequests\": 0,\n        \"delayBetweenRuns\": 10000,\n        \"transactionDistribution\": {\n          \"CUSTOMER_SESSION\": 70,\n          \"QUERY_DASHBOARD\": 75,\n          \"PRICE_UPDATE\": 82,\n          \"UPDATE_PRODUCT\": 85,\n          \"UPDATE_DELIVERY\": 100\n        },\n        \"concurrencyLevel\": 1,\n        \"ingestionConfig\": {\n          \"strategy\": \"WORKER_PER_CPU\",\n          \"concurrencyLevel\": 6,\n          \"mapTableToUrl\": {\n              \"sellers\": \"http://localhost:8080/seller\",\n              \"customers\": \"http://localhost:8080/customer\",\n              \"stock_items\": \"http://localhost:8080/stock\",\n              \"products\": \"http://localhost:8080/product\"\n          }\n        },\n        \"runs\": [\n          {\n            \"numProducts\": 1000,\n            \"customerDistribution\": \"UNIFORM\",\n            \"sellerDistribution\": \"UNIFORM\",\n            \"keyDistribution\": \"UNIFORM\"\n          },\n          //{\n          //  \"numProducts\": 1000,\n          //  \"customerDistribution\": \"UNIFORM\",\n          //  \"sellerDistribution\": \"ZIPFIAN\",\n          //  \"keyDistribution\": \"ZIPFIAN\"\n          //},\n          //{\n          //  \"numProducts\": 10000,\n          //  \"customerDistribution\": \"UNIFORM\",\n          //  \"sellerDistribution\": \"UNIFORM\",\n          //  \"keyDistribution\": \"UNIFORM\"\n          //},\n          //{\n          //  \"numProducts\": 10000,\n          //  \"customerDistribution\": \"UNIFORM\",\n          //  \"sellerDistribution\": \"ZIPFIAN\",\n          //  \"keyDistribution\": \"ZIPFIAN\"\n          //}\n        ],\n        \"postRunTasks\": [\n          {\n              \"name\": \"default\",\n              \"url\": \"http://localhost:8080/cleanup\"\n          }\n        ],\n        \"postExperimentTasks\": [\n          {\n              \"name\": \"default\",\n              \"url\": \"http://localhost:8080/cleanup\"\n          }\n        ],\n        \"customerWorkerConfig\": {\n          \"maxNumberKeysToBrowse\": 10,\n          \"maxNumberKeysToAddToCart\": 10,\n          \"minMaxQtyRange\": {\n            \"min\": 1,\n            \"max\": 10\n          },\n          \"delayBetweenRequestsRange\": {\n            \"min\": 1,\n            \"max\": 1000\n          },\n          \"checkoutProbability\": 100,\n          \"voucherProbability\": 5,\n          \"productUrl\": \"http://localhost:8080/product\",\n          \"cartUrl\": \"http://localhost:8080/cart\",\n          \"interactive\": false\n        },\n        \"sellerWorkerConfig\": {\n          \"interactive\": false,\n          \"delayBetweenRequestsRange\": {\n            \"min\": 1,\n            \"max\": 1000\n          },\n          \"adjustRange\": {\n            \"min\": 1,\n            \"max\": 10\n          },\n          \"sellerUrl\": \"http://localhost:8080/seller\",\n          \"productUrl\": \"http://localhost:8080/product\"\n        },\n        \"deliveryWorkerConfig\": {\n          \"shipmentUrl\": \"http://localhost:8080/shipment\"\n        }\n      }";

    [Fact]
    public void TestParse()
    {
        var experimentConfig = JsonConvert.DeserializeObject<ExperimentConfig>(json);
        Assert.True(experimentConfig.numCustomers > 0);
    }
}


