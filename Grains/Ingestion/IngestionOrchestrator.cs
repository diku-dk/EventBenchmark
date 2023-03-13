using System;
using Common.Ingestion;
using System.Threading.Tasks;
using Orleans;
using Common.Ingestion.DTO;
using System.Collections.Generic;
using GrainInterfaces.Ingestion;
using System.Linq;
using System.Net.Http;

namespace Grains.Ingestion
{
    /*
     * Orchestrates several grains
     * Partitions the workloads across several stateless grains to perform the work
     * I cannot be stateless, since we have to make sure only one source grain is generating the data
     * One per microservice
     */
    public class IngestionOrchestrator : Grain, IIngestionOrchestrator
    {

        private static readonly HttpClient client = new HttpClient();

        // less than threshold, no need to partition
        private readonly static int partitioningThreshold = 10;

        private readonly static string healthCheckKey = "healthCheck";

        GeneratedData data;

        private Status status = Status.NEW;

        private enum Status
        {
            NEW,
            IN_PROGRESS,
            FINISHED
        }

        public async override Task OnActivateAsync()
        {
            Console.WriteLine("Ingestion orchestrator on activate!");
            await base.OnActivateAsync();
            return;
        }

        public async Task<bool> Run(IngestionConfiguration config)
        {

            if (this.status == Status.IN_PROGRESS)
            {
                // return;
                Console.WriteLine("Ingestion orchestrator called again while in progress. Maybe bug?");
                return false;
            }
            if (this.status == Status.FINISHED) {
                Console.WriteLine("Ingestion orchestrator called again in the same context. Data will be reused.");
            }

            // health check. is the microservice online?
            if (!config.mapTableToUrl.ContainsKey(healthCheckKey))
            {
                Console.WriteLine("It was not possible to find the healthcheck API " + healthCheckKey);
                return false;
            }

            Console.WriteLine("Ingestion orchestrator called!");

            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, config.mapTableToUrl[healthCheckKey]);
            using HttpResponseMessage response = await Task.Run(() => client.SendAsync(message));
            
            Console.WriteLine("Health check status: "+response.StatusCode.ToString());
            if(!response.IsSuccessStatusCode)
            {
                Console.WriteLine("Healthcheck failed");
                return false;
            }

            if (this.status == Status.NEW || this.status == Status.FINISHED)
            {
                if(this.status == Status.NEW)
                {
                    if (config.dataNatureType == DataSourceType.SYNTHETIC)
                    {
                        data = SyntheticDataGenerator.Generate();
                    }
                    else
                    {
                        data = RealDataGenerator.Generate();
                    }
                    Console.WriteLine("Ingestion orchestrator data generated!");
                }
                this.status = Status.IN_PROGRESS;
            }

            if (config.partitioningStrategy == IngestionPartitioningStrategy.SINGLE_WORKER)
            {
                Console.WriteLine("Single worker will start");
                List<IngestionBatch> batches = new List<IngestionBatch>();
                foreach (var table in data.tables)
                {
                    if (!config.mapTableToUrl.ContainsKey(table.Key))
                    {
                        Console.WriteLine("It was not possible to find the URL for table " + table.Key);
                        continue;
                    }
                    string url = config.mapTableToUrl[table.Key];
                    IngestionBatch ingestionBatch = new IngestionBatch()
                    {
                        url = url,
                        data = table.Value
                    };
                    batches.Add(ingestionBatch);
                }
                IIngestionWorker worker = GrainFactory.GetGrain<IIngestionWorker>("SINGLE_WORKER");
                Console.WriteLine("Single worker will be dispatched");
                await worker.Send(batches);
                Console.WriteLine("Single worker finished");
            }
            else if (config.partitioningStrategy == IngestionPartitioningStrategy.TABLE_PER_WORKER)
            {
                await RunAsTablePerWorker(config, data);
            }
            else
            {
                // https://learn.microsoft.com/en-us/dotnet/csharp/programming-guide/concepts/linq/
                // https://stackoverflow.com/questions/11954608/count-values-in-dictionary-using-linq-and-linq-extensions
                int numberOfRecords = data.tables.Values.Sum(list => list.Count);
                if (numberOfRecords < partitioningThreshold)
                {
                    await RunAsTablePerWorker(config, data);
                }
                else
                {
                    int numberOfWorkers = config.numberCpus * 2;
                    int numberOfRecordsPerWorker = numberOfRecords / numberOfWorkers;
               
                    List<Task> taskList = new List<Task>();
                    foreach (var table in data.tables)
                    {
                        if(table.Value.Count > numberOfRecordsPerWorker)
                        {
                            int numberOfWorkersToAssign = table.Value.Count / numberOfRecordsPerWorker;
                            int indexInit;
                            for (int i = 0; i < numberOfWorkersToAssign; i++)
                            {
                                indexInit = i * numberOfRecordsPerWorker;
                                IIngestionWorker worker = GrainFactory.GetGrain<IIngestionWorker>(table.Key + "_" + indexInit);
                                IngestionBatch ingestionBatch = new IngestionBatch()
                                {
                                    url = config.mapTableToUrl[table.Key],
                                    data = table.Value.GetRange(indexInit, indexInit + numberOfRecordsPerWorker)
                                };
                                taskList.Add(worker.Send(ingestionBatch));
                            }
                            // optimization is putting more records from other table in the last worker...
                            // indexInit = (numberOfWorkersToAssign - 1) * numberOfRecordsPerWorker;
                            // countForWorker = table.Value.Count - indexInit;
                            
                        }
                        else
                        {
                            IIngestionWorker worker = GrainFactory.GetGrain<IIngestionWorker>(table.Key);
                            IngestionBatch ingestionBatch = new IngestionBatch()
                            {
                                url = config.mapTableToUrl[table.Key],
                                data = table.Value
                            };
                            taskList.Add(worker.Send(ingestionBatch));
                        }

                    }

                    await Task.WhenAll(taskList);

                }

            }

            this.status = Status.FINISHED;

            return true;
        }

        private async Task RunAsTablePerWorker(IngestionConfiguration config, GeneratedData data)
        {
            List<Task> taskList = new List<Task>();

            foreach (var table in data.tables)
            {
                IIngestionWorker worker = GrainFactory.GetGrain<IIngestionWorker>(table.Key);
                string url = config.mapTableToUrl[table.Key];
                IngestionBatch ingestionBatch = new IngestionBatch()
                {
                    url = url,
                    data = table.Value
                };
                taskList.Add(worker.Send(ingestionBatch));
            }

            await Task.WhenAll(taskList);
        }
    }
}
