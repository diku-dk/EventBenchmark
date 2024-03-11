using Common.Entities;
using Common.Metric;
using Common.Services;
using Common.Workload.Metrics;

namespace Tests.Anomaly;

public class AnomalyTest
{

    [Fact]
    public void TestSimpleAnomaly()
    {
        Dictionary<int, List<Product>> productUpdatesPerSeller = new();
        // y2 y1 x2 x1
        Product product1 = new Product(){ seller_id = 1, product_id = 1, price = 10, version = "0" };
        Product product2 = new Product(){ seller_id = 1, product_id = 1, price = 15, version = "0" };
        Product product3 = new Product(){ seller_id = 1, product_id = 2, price = 10, version = "0" };
        Product product4 = new Product(){ seller_id = 1, product_id = 2, price = 15, version = "0" };
        // the most recent come first
        List<Product> updates = new()
        {
            product4,
            product3,
            product2,
            product1,
        };
        productUpdatesPerSeller.Add( 1, updates );

        Dictionary<int, IDictionary<string,List<CartItem>>> cartHistoryPerCustomer = new();
        Dictionary<string,List<CartItem>> customerCartHistory = new();

        CartItem cartItem1 = new CartItem(){ Version="0", SellerId = 1, ProductId=1, UnitPrice = 10 };
        CartItem cartItem2 = new CartItem(){ Version="0", SellerId = 1, ProductId=2, UnitPrice = 10 };
        // first item in cart: y1. second item in cart: x1 (should have been x2 to meet causality)
        List<CartItem> cartItems = new(){ cartItem1, cartItem2 };
        customerCartHistory.Add("1", cartItems);
        cartHistoryPerCustomer.Add(1, customerCartHistory);

        int count = TestMetricManager.DoCollectReplicationAnomalies(productUpdatesPerSeller, cartHistoryPerCustomer);

        Assert.True(count == 1);

    }

    private class TestMetricManager : MetricManager
    {
        public TestMetricManager(ISellerService sellerService, ICustomerService customerService, IDeliveryService deliveryService) : base(sellerService, customerService, deliveryService)
        {
        }

        protected override List<Latency> CollectFromDelivery(DateTime finishTime)
        {
            throw new NotImplementedException();
        }
    }

}


