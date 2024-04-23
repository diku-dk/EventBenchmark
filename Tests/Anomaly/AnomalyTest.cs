using Common.Entities;
using Common.Metric;
using Common.Services;

namespace Tests.Anomaly;

public class AnomalyTest
{

    [Fact]
    public void TestNoAnomalyBasedOnDifferentProductUpdateVersion()
    {
        Dictionary<int, List<Product>> productUpdatesPerSeller = BuildBasicProductUpdatePerSeller();

        // modifying x2 to another version
        productUpdatesPerSeller[1][2].version = "1";

        Dictionary<int, IDictionary<string, List<CartItem>>> cartHistoryPerCustomer = new();
        Dictionary<string, List<CartItem>> customerCartHistory = new();

        CartItem cartItem1 = new CartItem() { SellerId = 1, ProductId = 1, UnitPrice = 10, Version = "0" };
        CartItem cartItem2 = new CartItem() { SellerId = 1, ProductId = 2, UnitPrice = 10, Version = "0" };
        // first item in cart: y1. second item in cart: x2 (causality relation is met)
        List<CartItem> cartItems = new() { cartItem1, cartItem2 };
        customerCartHistory.Add("1", cartItems);
        cartHistoryPerCustomer.Add(1, customerCartHistory);

        int count = TestMetricManager.DoCollectReplicationAnomalies(productUpdatesPerSeller, cartHistoryPerCustomer);

        Assert.True(count == 0);
    }

    [Fact]
    public void TestNoAnomalyBasedOnDifferentCartItemVersion()
    {
        Dictionary<int, List<Product>> productUpdatesPerSeller = BuildBasicProductUpdatePerSeller();

        Dictionary<int, IDictionary<string, List<CartItem>>> cartHistoryPerCustomer = new();
        Dictionary<string, List<CartItem>> customerCartHistory = new();

        CartItem cartItem1 = new CartItem() { SellerId = 1, ProductId = 1, UnitPrice = 10, Version = "1" };
        CartItem cartItem2 = new CartItem() { SellerId = 1, ProductId = 2, UnitPrice = 10, Version = "0" };
        // first item in cart: y1. second item in cart: x2 (causality relation is met)
        List<CartItem> cartItems = new() { cartItem1, cartItem2 };
        customerCartHistory.Add("1", cartItems);
        cartHistoryPerCustomer.Add(1, customerCartHistory);

        int count = TestMetricManager.DoCollectReplicationAnomalies(productUpdatesPerSeller, cartHistoryPerCustomer);

        Assert.True(count == 0);
    }

    [Fact]
    public void TestNoAnomalyBasedOnPrice()
    {
        Dictionary<int, List<Product>> productUpdatesPerSeller = BuildBasicProductUpdatePerSeller();

        Dictionary<int, IDictionary<string, List<CartItem>>> cartHistoryPerCustomer = new();
        Dictionary<string, List<CartItem>> customerCartHistory = new();

        CartItem cartItem1 = new CartItem() { SellerId = 1, ProductId = 1, UnitPrice = 15, Version = "0" };
        CartItem cartItem2 = new CartItem() { SellerId = 1, ProductId = 2, UnitPrice = 10, Version = "0" };
        // first item in cart: y1. second item in cart: x2 (causality relation is met)
        List<CartItem> cartItems = new() { cartItem1, cartItem2 };
        customerCartHistory.Add("1", cartItems);
        cartHistoryPerCustomer.Add(1, customerCartHistory);

        int count = TestMetricManager.DoCollectReplicationAnomalies(productUpdatesPerSeller, cartHistoryPerCustomer);

        Assert.True(count == 0);
    }

    [Fact]
    public void TestSimpleAnomaly()
    {
        Dictionary<int, List<Product>> productUpdatesPerSeller = BuildBasicProductUpdatePerSeller();

        Dictionary<int, IDictionary<string, List<CartItem>>> cartHistoryPerCustomer = new();
        Dictionary<string, List<CartItem>> customerCartHistory = new();

        CartItem cartItem1 = new CartItem() { SellerId = 1, ProductId = 1, UnitPrice = 10, Version = "0" };
        CartItem cartItem2 = new CartItem() { SellerId = 1, ProductId = 2, UnitPrice = 10, Version = "0" };
        // first item in cart: y1. second item in cart: x1 (should have been x2 to meet causality)
        List<CartItem> cartItems = new() { cartItem1, cartItem2 };
        customerCartHistory.Add("1", cartItems);
        cartHistoryPerCustomer.Add(1, customerCartHistory);

        int count = TestMetricManager.DoCollectReplicationAnomalies(productUpdatesPerSeller, cartHistoryPerCustomer);

        Assert.True(count == 1);
    }

    private static Dictionary<int, List<Product>> BuildBasicProductUpdatePerSeller()
    {
        Dictionary<int, List<Product>> productUpdatesPerSeller = new();
        // y2 y1 x2 x1
        Product product1 = new Product() { seller_id = 1, product_id = 1, price = 10, version = "0" };
        Product product2 = new Product() { seller_id = 1, product_id = 1, price = 15, version = "0" };
        Product product3 = new Product() { seller_id = 1, product_id = 2, price = 10, version = "0" };
        Product product4 = new Product() { seller_id = 1, product_id = 2, price = 15, version = "0" };
        // the most recent come first
        List<Product> updates = new()
        {
            product4,
            product3,
            product2,
            product1,
        };
        productUpdatesPerSeller.Add(1, updates);
        return productUpdatesPerSeller;
    }

    [Fact]
    public void TestDoubleAnomaly()
    {
        Dictionary<int, List<Product>> productUpdatesPerSeller = BuildComplexProductUpdatePerSeller();

        Dictionary<int, IDictionary<string, List<CartItem>>> cartHistoryPerCustomer = new();
        Dictionary<string, List<CartItem>> customerCartHistory = new();

        CartItem cartItem1 = new CartItem() { SellerId = 1, ProductId = 1, UnitPrice = 10, Version = "0" };
        CartItem cartItem2 = new CartItem() { SellerId = 1, ProductId = 2, UnitPrice = 10, Version = "0" };
        // first item in cart: y1. second item in cart: x1 (should have been x2 to meet causality)
        List<CartItem> cartItems = new() { cartItem1, cartItem2 };
        customerCartHistory.Add("1", cartItems);
        cartHistoryPerCustomer.Add(1, customerCartHistory);

        int count = TestMetricManager.DoCollectReplicationAnomalies(productUpdatesPerSeller, cartHistoryPerCustomer);

        Assert.True(count == 2);
    }

    /**
     * A complex shows an item's anomaly may be counted twice given the causality has been broken twice
     */
    private static Dictionary<int, List<Product>> BuildComplexProductUpdatePerSeller()
    {
        Dictionary<int, List<Product>> productUpdatesPerSeller = new();
        // y2 y1 x3 x2 x1
        Product product1 = new Product() { seller_id = 1, product_id = 1, price = 10, version = "0" };
        Product product2 = new Product() { seller_id = 1, product_id = 1, price = 15, version = "0" };
        Product product3 = new Product() { seller_id = 1, product_id = 1, price = 20, version = "0" };
        Product product4 = new Product() { seller_id = 1, product_id = 2, price = 10, version = "0" };
        Product product5 = new Product() { seller_id = 1, product_id = 2, price = 15, version = "0" };
        // the most recent come first
        List<Product> updates = new()
        {
            product5,
            product4,
            product3,
            product2,
            product1,
        };
        productUpdatesPerSeller.Add(1, updates);
        return productUpdatesPerSeller;
    }

    private class TestMetricManager : MetricManager
    {
        public TestMetricManager(ISellerService sellerService, ICustomerService customerService, IDeliveryService deliveryService) : base(sellerService, customerService, deliveryService)
        {
        }
    }

}


