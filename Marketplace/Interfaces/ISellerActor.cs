using System;
using Common.Scenario.Entity;
using Marketplace.Infra;
using Orleans;
using Orleans.Concurrency;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Marketplace.Interfaces
{
    /**
     * Based on the olist API: https://dev.olist.com/docs/updating-price-and-stock
     * We don't model seller order history (https://dev.olist.com/docs/orders-grouping-by-shipping_limite_date)
     * because this history covers the entire shipment process, which is ommitted in our benchmark
     * Instead, we store seller logs. For each seller, a list of log entries reflecting important
     * actions are kept, so a seller can potentially review all the operations that have
     * been performed against the application
     */
    public interface ISellerActor : IGrainWithIntegerKey, SnapperActor
    {
        [AlwaysInterleave]
        public Task DeleteProduct(long productId);

        public Task UpdatePrices(IList<Product> products);

        public Task IncreaseStock(long productId, int quantity);

        // TODO discuss. should be a rare transaction.
        // online query? https://dev.olist.com/docs/get-financial-report
        // public Task RetrieveFinancialReport(int reference_year, int reference_month);
        // public Task RetrieveInFluxOrders...

        // TODO discuss. online query. not rare.
        // get open deliveries, in-progress orders, items below threshold, current reserved items, items being browsed and in carts, orders not delivered ordered by date
        // public Task GetOverview();

        // API
        public Task Init(Seller seller);

        public Task<Seller> GetSeller();

        public Task<IList<Product>> GetProducts();
    }
}

