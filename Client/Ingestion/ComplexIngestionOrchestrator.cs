using System;
using Common.Ingestion;
using System.Threading.Tasks;
using Orleans;
using Common.Ingestion.DTO;
using System.Collections.Generic;
using GrainInterfaces.Ingestion;
using System.Linq;
using System.Net.Http;
using Common.Serdes;
using Common.Http;
using System.Collections.Concurrent;
using Microsoft.VisualBasic;
using Orleans.Streams;
using Common.Streaming;
using Client.Ingestion.Config;

namespace Grains.Ingestion
{
    /*
     * Orchestrates several grains
     * Partitions the workloads across several stateless grains to perform the work
     * I cannot be stateless, since we have to make sure only one source grain is generating the data
     * One per microservice
     * TODO rethink using sync work lib: https://github.com/OrleansContrib/Orleans.SyncWork
     */
    public class ComplexIngestionOrchestrator
    {

        // if lower than the threshold, no need to partition
        private readonly static int partitioningThreshold = 10;

        private GeneratedData data;

        private Status status;

        private long guid;

        private IStreamProvider streamProvider;

        private IngestionConfig config;

        private enum Status
        {
            NEW,
            IN_PROGRESS,
            FINISHED
        }

        List<Task> taskList = new();

        private IDisposable timer;

        /*
        public override async Task OnActivateAsync()
        {
            this.guid = this.GetPrimaryKeyLong();
            this.streamProvider = this.GetStreamProvider(StreamingConfiguration.DefaultStreamProvider);
            var streamIncoming = streamProvider.GetStream<int>(StreamingConfiguration.IngestionStreamId, this.guid.ToString());

            var subscriptionHandles = await streamIncoming.GetAllSubscriptionHandles();
            if (subscriptionHandles.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles)
                {
                    await subscriptionHandle.ResumeAsync(Run);
                }
            }

            await streamIncoming.SubscribeAsync(Run);

            Console.WriteLine("Ingestion orchestrator activated!");
        }
        */

        public Task Init(IngestionConfig config)
        {
            this.status = Status.NEW;
            this.config = config;
            return Task.CompletedTask;
        }

        /**
         * This method may take arbitrary amount of time. Better to resort to Orleans streams.
         */

        /*
        private async Task Run(int obj, StreamSequenceToken token = null)
        {

            if (this.status == Status.IN_PROGRESS)
            {
                // this only happens if master publishes the event again...
                throw new Exception("Ingestion orchestrator called again while in progress. Maybe bug?");
            }

            // =====================

            Console.WriteLine("Ingestion orchestrator will start ingestion process!");

            //if (this.status == Status.NEW)
            //{
            //    if (config.dataSourceType == DataSourceType.SYNTHETIC)
            //    {
            //        data = SyntheticDataGenerator.Generate(SerdesFactory.build());
            //    }
            //    else
            //    {
            //        data = RealDataGenerator.Generate();
            //    }
            //    Console.WriteLine("Ingestion orchestrator data generated!");
            //}
            this.status = Status.IN_PROGRESS;
            

            if (config.distributionStrategy == IngestionDistributionStrategy.SINGLE_WORKER)
            {
                // just perform tasks here, no need to create worker grain
                foreach (var table in data.tables)
                {

                    if (!config.mapTableToUrl.ContainsKey(table.Key))
                    {
                        Console.WriteLine("It was not possible to find the URL for table " + table.Key);
                        continue;
                    }
                    string url = config.mapTableToUrl[table.Key];

                    foreach (var item in table.Value)
                    {
                        await (Task.Run(() =>
                        {
                            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, url);
                            message.Content = HttpUtils.BuildPayload(item);
                           
                            try
                            {
                                using HttpResponseMessage response = HttpUtils.client.Send(message);
                            }
                            catch (HttpRequestException e)
                            {
                                Console.WriteLine("Exception message: {0}", e.Message);
                            }
                            
                        }));
                    }
                }

                await SignalCompletion();
                return;

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

                        if (!config.mapTableToUrl.ContainsKey(table.Key))
                        {
                            Console.WriteLine("It was not possible to find the URL for table " + table.Key);
                            continue;
                        }

                        string url = config.mapTableToUrl[table.Key];

                        if (table.Value.Count > numberOfRecordsPerWorker)
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

                }

            }

            Console.WriteLine("Ingestion orchestrator dispatched all workers!");

            // setup timer according to the config passed. the timer defines the end of the experiment
            this.timer = this.RegisterTimer(CheckTermination, null, TimeSpan.FromMilliseconds(2000), TimeSpan.FromMilliseconds(5000));

            return; // Task.CompletedTask;
        }
        */

        /*
        private void RunAsTablePerWorker(IngestionConfiguration config, GeneratedData data)
        {
            foreach (var table in data.tables)
            {
                IIngestionWorker worker = GrainFactory.GetGrain<IIngestionWorker>(table.Key);
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
                taskList.Add(worker.Send(ingestionBatch));
            }
        }
        */

        private async Task CheckTermination(object arg)
        {

            for(int i = 0; i < taskList.Count; i++)
            {
                if(!taskList.ElementAt(i).IsCompleted)
                {
                    return;
                }
            }

            await SignalCompletion();

            // dispose timer
            this.timer.Dispose();

            return;
        }

        private async Task SignalCompletion()
        {
            this.status = Status.FINISHED;
            Console.WriteLine("Ingestion process has finished.");
            // send the event to master
            var resultStream = streamProvider.GetStream<int>(StreamingConstants.IngestionStreamId, "master");
            await resultStream.OnNextAsync(1);
        }

    }
}
