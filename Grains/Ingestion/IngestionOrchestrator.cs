using System;
using Common.Ingestion;
using System.Threading.Tasks;
using Orleans;
using Common.Ingestion.DTO;
using System.Collections.Generic;
using GrainInterfaces.Ingestion;
using System.Linq;
using System.Net.Http;
using Common.Ingestion.Config;
using Common.Ingestion.DataGeneration;
using Common.Serdes;
using Common.Http;
using System.Collections.Concurrent;

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

        List<Task> taskList = new();

        public async override Task OnActivateAsync()
        {
            Console.WriteLine("Ingestion orchestrator on activate!");
            await base.OnActivateAsync();
            return;
        }

        /**
         * This method may takr arbitrary amount of time. Better to resort to Orleans streams.
         */
        public async Task<bool> Run(IngestionConfiguration config)
        {

            if (this.status == Status.IN_PROGRESS)
            {
                // this only happens if reentrancy enbaled for this method
                Console.WriteLine("Ingestion orchestrator called again while in progress. Disable reentrancy or maybe bug?");
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

            Console.WriteLine("Ingestion orchestrator started!");

            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, config.mapTableToUrl[healthCheckKey]);
            using HttpResponseMessage response = await Task.Run(() => HttpUtils.client.SendAsync(message));

            Console.WriteLine("Health check status code: " + response.StatusCode.ToString());
            if (!response.IsSuccessStatusCode)
            {
                Console.WriteLine("Healthcheck failed");
                return false;
            }

            // =====================

            
            if(this.status == Status.NEW)
            {
                if (config.dataNatureType == DataSourceType.SYNTHETIC)
                {
                    data = SyntheticDataGenerator.Generate(SerdesFactory.build());
                }
                else
                {
                    data = RealDataGenerator.Generate();
                }
                Console.WriteLine("Ingestion orchestrator data generated!");
            }
            this.status = Status.IN_PROGRESS;
            

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
                taskList.Add( worker.Send(batches) ); // FIXME probably exception here related to timeout

                // trackedWorkerTasks[0].IsCompleted

                Console.WriteLine("Single worker dispatching finished");
            }
            else if (config.partitioningStrategy == IngestionPartitioningStrategy.TABLE_PER_WORKER)
            {
                RunAsTablePerWorker(config, data);
            }
            else
            {
                // https://learn.microsoft.com/en-us/dotnet/csharp/programming-guide/concepts/linq/
                // https://stackoverflow.com/questions/11954608/count-values-in-dictionary-using-linq-and-linq-extensions
                int numberOfRecords = data.tables.Values.Sum(list => list.Count);
                if (numberOfRecords < partitioningThreshold)
                {
                    RunAsTablePerWorker(config, data);
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

                    // await Task.WhenAll(taskList);

                }

            }

            this.status = Status.FINISHED;
            Console.WriteLine("Ingestion orchestrator dispatched all workers!");

            return true;
        }

        private void RunAsTablePerWorker(IngestionConfiguration config, GeneratedData data)
        {
            // List<Task> taskList = new List<Task>();

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

            // await Task.WhenAll(taskList);
        }


        public Task<int> GetStatus()
        {

            for(int i = 0; i < taskList.Count; i++)
            {
                if(!taskList.ElementAt(i).IsCompleted)
                {
                    return Task.FromResult(0);
                }
            }

            return this.status == Status.FINISHED ? Task.FromResult(1) : Task.FromResult(0);
        }

    }
}
