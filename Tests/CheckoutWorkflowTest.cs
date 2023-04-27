using System;
using Orleans.TestingHost;
using System.Threading.Tasks;
using Xunit;
using Marketplace.Interfaces;
using System.Text;
using Common.Entity;
using Marketplace.Infra;
using System.Collections.Generic;

namespace Marketplace.Test
{
    [Collection(ClusterCollection.Name)]
    public class CheckoutWorkflowTest
	{
        private readonly TestCluster _cluster;

        private readonly Random random = new Random();

        public CheckoutWorkflowTest(ClusterFixture fixture)
        {
            _cluster = fixture.Cluster;
        }

        [Fact]
        public async Task Checkout()
        {
            // initialize default metadata
            var metadata = _cluster.GrainFactory.GetGrain<IMetadataGrain>(0);
            await metadata.Init(ActorSettings.GetDefault());

            // load customer in customer actor
            var customer = _cluster.GrainFactory.GetGrain<ICustomerActor>(0);
            await customer.AddCustomer(new Customer()
            {
                id = 0,
                first_name = "",
                last_name = "",
                address = "",
                complement = "",
                birth_date = "",
                zip_code_prefix = "",
                city = "",
                state = "",
                pending_deliveries_count = 0,
                abandoned_cart_count = 0,
                delivery_count = 0,
                failed_payment_count = 0,
                success_payment_count = 0,
                total_spent_freights = 0,
                total_spent_items = 0
            });

            var cart = _cluster.GrainFactory.GetGrain<ICartActor>(0);
            await cart.AddProduct(GenerateBasketItem(1));
            await cart.AddProduct(GenerateBasketItem(2));

            // add correspondent stock items
            var stock = _cluster.GrainFactory.GetGrain<IStockActor>(0);
            await stock.AddItem(new StockItem()
            {
                product_id = 1,
                seller_id = 1,
                qty_available = 1,
                qty_reserved = 0,
                order_count = 0,
                ytd = 1,
            });
            await stock.AddItem(new StockItem()
            {
                product_id = 2,
                seller_id = 1,
                qty_available = 1,
                qty_reserved = 0,
                order_count = 0,
                ytd = 1,
            });

            CustomerCheckout customerCheckout = new()
            {
                CustomerId = 0,
                FirstName = "",
                LastName = "",
                Street = "",
                Complement = "",
                City = "",
                State = "",
                ZipCode = "",
                PaymentType = PaymentType.CREDIT_CARD.ToString(),
                CardNumber = random.Next().ToString(),
                CardHolderName = "",
                CardExpiration = "",
                CardSecurityNumber = "",
                CardBrand = "",
                Installments = 1,
                Vouchers = null
            };

            await cart.Checkout(customerCheckout);

            var order = _cluster.GrainFactory.GetGrain<IOrderActor>(0);
            List<Order> orders = await order.GetOrders(0, null);

            Assert.Single(orders);
        }

        private BasketItem GenerateBasketItem(long id, long sellerId = 1)
        {
            return new()
            {
                ProductId = id,
                SellerId = sellerId,
                 UnitPrice = random.Next(),
                 // OldUnitPrice = null,
                 FreightValue = 0,
                 Quantity = 1
            };
        }

       
    }
}

