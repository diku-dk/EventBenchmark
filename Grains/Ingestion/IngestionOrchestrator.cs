using System;
using Common.Ingestion;
using System.Threading.Tasks;
using Orleans;
using System.Text.Json;
using Common.Ingestion.DTO;
using System.Collections.Generic;
using GrainInterfaces.Ingestion;
using System.Runtime.ConstrainedExecution;
using System.ComponentModel.Design;
using Orleans.Concurrency;
using System.Threading;
using System.Linq;

namespace Grains.Ingestion
{
    /*
     * orchestrates several grains
     * partition the workloads across several statless grains to perform the work
     * cannot be stateless, since we have to make sure only one source grain is generating the data
     * One per microservice
     */
    public class IngestionOrchestrator : Grain, IGrainWithIntegerKey
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
            return;
        }

        public async Task Run(IngestionConfiguration config)
        {

            if(this.status == Status.IN_PROGRESS) {
                return;
            }
            if(this.status == Status.NEW || this.status == Status.FINISHED)
            {
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

            if(config.partitioningStrategy == IngestionPartitioningStrategy.TABLE_PER_WORKER)
            {
                await runAsTablePerWorker(config, data);

                // foreach (Task task in taskList) await task;

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

                    List<string> records = new List<string>();

                    int count = 0;
                    foreach (var table in data.tables)
                    {



                    }

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
