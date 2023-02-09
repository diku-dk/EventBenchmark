using System;
using Common.Ingestion;
using System.Threading.Tasks;
using Orleans;
using Common.Ingestion.DTO;
using System.Collections.Generic;
using GrainInterfaces.Ingestion;
using System.Linq;

namespace Grains.Ingestion
{
    /*
     * orchestrates several grains
     * partition the workloads across several statless grains to perform the work
     * cannot be stateless, since we have to make sure only one source grain is generating the data
     * One per microservice
     */
    public class IngestionOrchestrator : Grain, IIngestionOrchestrator
    {
        // less than threshold, no need to partition
        private static int partitioningThreshold = 10;

        private Status status;

        private enum Status
        {
            NEW,
            IN_PROGRESS,
            FINISHED
        }

        public async override Task OnActivateAsync()
        {
            this.status = Status.NEW;
            Console.WriteLine("Ingestion orchestrator on activate!");
            await base.OnActivateAsync();
            return;
        }

        public async Task Run(IngestionConfiguration config)
        {

            if(this.status == Status.IN_PROGRESS) {
                // return;
                Console.WriteLine("Ingestion orchestrator called again in the same context!");
            }
            if(this.status == Status.NEW || this.status == Status.FINISHED)
            {
                Console.WriteLine("Ingestion orchestrator called!");
                this.status = Status.IN_PROGRESS;
            }

            GeneratedData data;

            if (config.dataNatureType == DataSourceType.SYNTHETIC) 
            {
                data = SyntheticDataGenerator.Generate();
            } else
            {
                data = RealDataGenerator.Generate();
            }

            Console.WriteLine("Ingestion orchestrator data generated!");

            if (config.partitioningStrategy == IngestionPartitioningStrategy.SINGLE_WORKER)
            {

                List<IngestionBatch> batches = new List<IngestionBatch>();
                foreach (var table in data.tables)
                {
                    string url = config.mapTableToUrl[table.Key];
                    IngestionBatch ingestionBatch = new IngestionBatch()
                    {
                        url = url,
                        data = table.Value
                    };
                    batches.Add(ingestionBatch);
                }

                IIngestionWorker worker = GrainFactory.GetGrain<IIngestionWorker>("NONE");
                await worker.Send(batches);

            }
            else if (config.partitioningStrategy == IngestionPartitioningStrategy.TABLE_PER_WORKER)
            {
                await runAsTablePerWorker(config, data);
            }
            else
            {

                // https://learn.microsoft.com/en-us/dotnet/csharp/programming-guide/concepts/linq/
                // https://stackoverflow.com/questions/11954608/count-values-in-dictionary-using-linq-and-linq-extensions
                int numberOfRecords = data.tables.Values.Sum(list => list.Count);
                if (numberOfRecords < partitioningThreshold)
                {
                    await runAsTablePerWorker(config, data);
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


            // TODO check correctness... make get requests looking for some random IDs. also total sql to count total of items ingested
            

            this.status = Status.FINISHED;

        }

        private async Task runAsTablePerWorker(IngestionConfiguration config, GeneratedData data)
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
