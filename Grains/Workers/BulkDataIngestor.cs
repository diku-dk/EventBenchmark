using System;
using Orleans;

namespace Grains.Workers
{
    // cannot be stateless, since we have to make sure only one source grain is generating the data

    // orchestrates several grains
    // partition the workloads across several statless grains to perform the work

    public class BulkDataIngestor : Grain
    {

        public BulkDataIngestor()
        {

        }




    }
}
