using System.Collections.Concurrent;
using Common.Entities;
using Common.Infra;
using Common.Workers;
using Common.Workload;
using Common.Workload.Seller;
using Microsoft.Extensions.Logging;

namespace Tests.Dapr;

public class DaprTest
{

    private static readonly ConcurrentQueue<Message> messages = new ConcurrentQueue<Message>();

    private static int numThreads = 100;

	[Fact]
	public async void TestSellerLinearizable()
	{
		// the goal is to check there are no interleaving of divergent versions

		//                           T2                                      T1
		// example: ----- price update with version 1 -------- product update to version 2
		// cannot send the price update (v1) after the product update (v2) above
        var logger = LoggerProxy.GetInstance("SellerThread_"+ 1);
        var testSeller = new TestSeller(1, null, new SellerWorkerConfig(){ adjustRange = new Interval(1,10) }, logger);

        testSeller.SetUp( new List<Product>(){ new Product(){ product_id = 1, price = 10, version = 0 },
            //    new Product(){ product_id = 2, price = 10, version = 0 }, }
        }, Common.Distribution.DistributionType.UNIFORM );

        var random = new Random();

        var tasks = new List<Task>(100);

        for(int i = 1; i <= numThreads; i++)
        {
            var toPass = i;
            if(random.Next(0,3) == 0){
                tasks.Add( Task.Run(()=>testSeller.UpdatePrice(toPass)) );
            } else
            {
                tasks.Add( Task.Run(()=>testSeller.UpdateProduct(toPass)) );
            }
        }

        await Task.WhenAll(tasks);

        // assert whether there are no bad interleavings
 
        var list = messages.ToList();
        if(list[0].type == TransactionType.PRICE_UPDATE){
            // start with price update
            Assert.True(list[0].version == 0);
        }

        for(int i = 1; i < 100; i++)
        {
            if(list[i].type == TransactionType.PRICE_UPDATE && list[i-1].type == TransactionType.UPDATE_PRODUCT)
            {
                Assert.True(list[i].version == list[i-1].version);
            }
        }

	}

    private struct Message
    {
        public TransactionType type;
        // public int productId;
        public int tid;
        public int version;

        public Message(TransactionType type, int tid, int version) : this()
        {
            this.type = type;
            this.tid = tid;
            this.version = version;
        }
    }

    protected class TestSeller : AbstractSellerThread
    {
        public TestSeller(int sellerId, HttpClient httpClient, SellerWorkerConfig workerConfig, ILogger logger) : base(sellerId, httpClient, workerConfig, logger)
        {
        }

        protected override void SendProductUpdateRequest(Product product, int tid)
        {
            messages.Enqueue(new Message(TransactionType.UPDATE_PRODUCT, tid, product.version));
        }

        protected override void SendUpdatePriceRequest(int tid, Product productToUpdate, float newPrice)
        {
            messages.Enqueue(new Message(TransactionType.PRICE_UPDATE, tid, productToUpdate.version));
        }
    }

}

