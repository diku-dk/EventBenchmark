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
using Microsoft.VisualBasic;
using Orleans.Streams;
using Common.Streaming;

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

        private GeneratedData data;

        private Status status;//  = Status.NEW;

        private long guid;

        private IStreamProvider streamProvider;

        private IngestionConfiguration config;

        private enum Status
        {
            NEW,
            IN_PROGRESS,
            FINISHED
        }

        List<Task> taskList = new();

        public override async Task OnActivateAsync()
        {
            Console.WriteLine("Ingestion orchestrator on activate!");

            this.guid = this.GetPrimaryKeyLong();
            this.streamProvider = this.GetStreamProvider(StreamingConfiguration.DefaultStreamProvider);
            var streamIncoming = streamProvider.GetStream<object>(StreamingConfiguration.IngestionStreamId, this.guid.ToString());

            var subscriptionHandles = await streamIncoming.GetAllSubscriptionHandles();
            if (subscriptionHandles.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles)
                {
                    await subscriptionHandle.ResumeAsync(Run);
                }
            }

            await streamIncoming.SubscribeAsync(Run);
        }

        public Task Init(IngestionConfiguration config)
        {
            this.status = Status.NEW;
            this.config = config;
            return Task.CompletedTask;
        }

        /**
         * This method may take arbitrary amount of time. Better to resort to Orleans streams.
         */
        private Task Run(object obj, StreamSequenceToken? token = null)
        {

            if (this.status == Status.IN_PROGRESS)
            {
                // this only happens if master publishes the event again...
                Console.WriteLine("Ingestion orchestrator called again while in progress. Maybe bug?");
                // return;
            }

            // =====================

            Console.WriteLine("Ingestion orchestrator will start ingestion process!");

            if (this.status == Status.NEW)
            {
                if (config.dataSourceType == DataSourceType.SYNTHETIC)
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
            

            if (config.distributionStrategy == IngestionDistributionStrategy.SINGLE_WORKER)
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
            else if (config.distributionStrategy == IngestionDistributionStrategy.TABLE_PER_WORKER)
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

            return Task.CompletedTask;
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
