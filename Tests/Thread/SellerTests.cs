using Common.Entities;
using Common.Infra;
using Common.Workers.Seller;
using Common.Workload;
using Common.Workload.Seller;
using Microsoft.Extensions.Logging;

namespace Tests.Thread;

public class SellerTests
{

    private static int numThreads = 100;
    private static readonly Random random = new Random();

    private static readonly Queue<Message>[] queues = { new Queue<Message>(), new Queue<Message>() };

    [Fact]
	public async void TestTwoProductsLinearizability()
	{
        var logger = LoggerProxy.GetInstance("SellerThread_"+ 1);
        var testSeller = new TestTwoProductsSeller(1, new SellerWorkerConfig(){ adjustRange = new Interval(1,10) }, logger);

        testSeller.SetUp( new List<Product>(){ new Product(){ product_id = 1, price = 10, version = 0 },
                                               new Product(){ product_id = 2, price = 10, version = 0 } },
                          Common.Distribution.DistributionType.UNIFORM );

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

        for(int j = 0; j < 2; j++){
            var list = queues[j].ToList();
            if(list[0].type == TransactionType.PRICE_UPDATE){
                // start with price update
                Assert.True(list[0].version == 0);
            }

            for(int i = 1; i < list.Count; i++)
            {
                if( (list[i].type == TransactionType.PRICE_UPDATE && list[i-1].type == TransactionType.UPDATE_PRODUCT) ||
                    (list[i].type == TransactionType.PRICE_UPDATE && list[i-1].type == TransactionType.PRICE_UPDATE) )
                {
                    Assert.True(list[i].version == list[i-1].version);
                } else if ( (list[i].type == TransactionType.UPDATE_PRODUCT && list[i-1].type == TransactionType.UPDATE_PRODUCT) ||
                            (list[i].type == TransactionType.UPDATE_PRODUCT && list[i-1].type == TransactionType.PRICE_UPDATE) )
                {
                    Assert.True(list[i].version != list[i-1].version);
                }
            }
        }

    }

    protected class TestTwoProductsSeller : AbstractSellerThread
    {
        public TestTwoProductsSeller(int sellerId, SellerWorkerConfig workerConfig, ILogger logger) : base(sellerId, workerConfig, logger)
        {
        }

        public override void BrowseDashboard(int tid)
        {
            throw new NotImplementedException();
        }

        protected override void SendProductUpdateRequest(Product product, int tid)
        {
            queues[product.product_id - 1].Enqueue(new Message(TransactionType.UPDATE_PRODUCT, tid, product.version));
        }

        protected override void SendUpdatePriceRequest(int tid, Product productToUpdate, float newPrice)
        {
            queues[productToUpdate.product_id - 1].Enqueue(new Message(TransactionType.PRICE_UPDATE, tid, productToUpdate.version));
        }
    }

    // ======================================================================================================= //

    private static readonly Queue<Message> messages = new Queue<Message>();

	[Fact]
	public async void TestSingleProductLinearizability()
	{
		// the goal is to check there are no interleaving of divergent versions

		//                           T2                                      T1
		// example: ----- price update with version 1 -------- product update to version 2
		// cannot send the price update (v1) after the product update (v2) above
        var logger = LoggerProxy.GetInstance("SellerThread_"+ 1);
        var testSeller = new TestSingleProductSeller(1, new SellerWorkerConfig(){ adjustRange = new Interval(1,10) }, logger);

        testSeller.SetUp( new List<Product>(){ new Product(){ product_id = 1, price = 10, version = 0 },
            //    new Product(){ product_id = 2, price = 10, version = 0 }, }
        }, Common.Distribution.DistributionType.UNIFORM );

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

        for(int i = 1; i < numThreads; i++)
        {
            if( (list[i].type == TransactionType.PRICE_UPDATE && list[i-1].type == TransactionType.UPDATE_PRODUCT) ||
                (list[i].type == TransactionType.PRICE_UPDATE && list[i-1].type == TransactionType.PRICE_UPDATE) )
            {
                Assert.True(list[i].version == list[i-1].version);
            } else if ( (list[i].type == TransactionType.UPDATE_PRODUCT && list[i-1].type == TransactionType.UPDATE_PRODUCT) ||
                        (list[i].type == TransactionType.UPDATE_PRODUCT && list[i-1].type == TransactionType.PRICE_UPDATE) )
            {
                Assert.True(list[i].version != list[i-1].version);
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

    protected class TestSingleProductSeller : AbstractSellerThread
    {
        public TestSingleProductSeller(int sellerId, SellerWorkerConfig workerConfig, ILogger logger) : base(sellerId, workerConfig, logger)
        {
        }

        public override void BrowseDashboard(int tid)
        {
            throw new NotImplementedException();
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

