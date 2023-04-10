using System;
using Common.Entity;
using System.Threading.Tasks;
using System.Collections.Generic;
using Common.Scenario.Entity;
using Orleans;
using Marketplace.Infra;
using System.Linq;
using System.Collections;
using Orleans.Runtime;
using Marketplace.Message;
using System.Net.NetworkInformation;

namespace Marketplace.Actor
{
    /**
     * https://olist.com/pt-br/solucoes-para-comercio/vender-em-marketplaces/
     * "A tabela de frete é baseada na região em que o lojista está e 
     * também no peso do produto. Dessa forma, se o consumidor for da região 
     * norte e o pedido for expedido da região sudeste, o lojista pagará o 
     * valor de frete tabelado para a região sudeste."
     * Order details: https://dev.olist.com/docs/orders
     * Logistic details: https://dev.olist.com/docs/fulfillment
     * "The order items must be shipped in unitary packages. 
     * For no reason order items should be packaged together in the same box."
     */
    public interface IShipmentActor : IGrainWithIntegerKey, SnapperActor
    {
        public Task<Dictionary<long, decimal>> GetQuotation(string customerZipCode);

        public Task<decimal> GetQuotation(string from, string to);

        public Task ProcessShipment(Invoice invoice);

        public Task UpdatePackageDelivery(long shipment_id, int package_id);

        // retrieve the packages not delivered yet
        public Task<List<Package>> GetOpenPackagesBySeller(long seller_id);
    }

    public class ShipmentActor : Grain, IShipmentActor
    {
        private long nextShipmentId;
        // PK
        private Dictionary<long, Shipment> shipments;
        // other table of shipment
        private Dictionary<long, List<Package>> packages;
        private long nCustPartitions;
        private long shipmentActorId;

        public ShipmentActor()
        {
            this.nextShipmentId = 1;
        }

        public override async Task OnActivateAsync()
        {
            this.shipmentActorId = this.GetPrimaryKeyLong();
            var mgmt = GrainFactory.GetGrain<IManagementGrain>(0);
            // https://github.com/dotnet/orleans/pull/1772
            // https://github.com/dotnet/orleans/issues/8262
            // GetGrain<IManagementGrain>(0).GetHosts();
            // 
            var stats = await mgmt.GetDetailedGrainStatistics(); // new[] { "ProductActor" });
            this.nCustPartitions = stats.Where(w => w.GrainType.Contains("CustomerActor")).Count();
        }

        public Task<Dictionary<long, decimal>> GetQuotation(string customerZipCode)
        {
            // from a table of combinations, seller to another zipcode, build the cost for each item
            // then sum
            return null;
        }

        public Task<decimal> GetQuotation(string from, string to)
        {
            throw new NotImplementedException();
        }

        /**
         * Packages are grouped by seller
         * Sellers with more items come first
         * When draw, use seller_id to undraw
         * In-memory, application-level aggregate operation
         * Inspired by: 
         * (i) https://dev.olist.com/docs/fulfillment
         * (ii) https://dev.olist.com/docs/retrieving-shipments-informations
         */
        public async Task ProcessShipment(Invoice invoice)
        {
            // aggregate per seller
            Dictionary<long, int> sellerDeliveryIdMap = new();
            int package_id = 1;

            // https://stackoverflow.com/questions/19517707/sort-list-based-on-group-count
            var items = invoice.items
                        .GroupBy(x => x.seller_id)
                        .OrderByDescending(g => g.Count())
                        .SelectMany(x => x).ToList();

            // create the shipment
            long today = System.DateTime.Now.Millisecond;
            Shipment shipment = new()
            {
                shipment_id = nextShipmentId,
                order_id = invoice.order.id,
                customer_id = invoice.order.customer_id,
                package_count = items.Count,
                total_freight_value = items.Sum(i => i.freight_value),
                request_date = invoice.order.purchase_timestamp,
                status = PackageStatus.created.ToString(),
                first_name = invoice.customer.FirstName,
                last_name = invoice.customer.LastName,
                street = invoice.customer.Street,
                complement = invoice.customer.Complement,
                zip_code_prefix = invoice.customer.ZipCode,
                city = invoice.customer.City,
                state = invoice.customer.State
            };

            List<Package> packages_ = new(items.Count);
            foreach (var item in items)
            {

                Package package = new()
                {
                    shipment_id = nextShipmentId,
                    package_id = package_id,
                    status = PackageStatus.shipped.ToString(),
                    freight_value = item.freight_value,
                    shipping_date = today,
                    seller_id = item.seller_id,
                    product_id = item.product_id,
                    quantity = item.quantity
                };

                packages_.Add(package);

                package_id++;

            }

            shipments.Add(shipment.shipment_id, shipment);
            packages.Add(shipment.shipment_id, packages_);

            nextShipmentId++;

            /**
             * Based on olist (https://dev.olist.com/docs/orders), the status of the order is
             * shipped when "at least one order item has been shipped"
             */
            IOrderActor orderActor = GrainFactory.GetGrain<IOrderActor>(invoice.orderActorId);
            await orderActor.UpdateOrderStatus(invoice.order.id, OrderStatus.SHIPPED);

        }

        /**
         * Inspired by: https://dev.olist.com/docs/retrieving-packages-informations
         * Index-based operation
         */
        public Task<List<Package>> GetOpenPackagesBySeller(long seller_id)
        {
            var sellerOpenPackages = packages.Values.SelectMany(p => p)
                .Where(p => p.status == PackageStatus.shipped.ToString() && p.seller_id == seller_id).ToList();

            return Task.FromResult(sellerOpenPackages);                   
                            
        }

        /**
         * Order status is "delivered" if at least one order item has been delivered
         * Based on https://dev.olist.com/docs/orders
         */
        public async Task UpdatePackageDelivery(long shipment_id, int package_id)
        {
            // aggregate operation
            var countDelivered = packages[shipment_id].Where(p => p.status == PackageStatus.delivered.ToString()).Count();

            Package toUpdate = packages[shipment_id].Where(p => p.package_id == package_id).First();

            // update status
            toUpdate.status = PackageStatus.delivered.ToString();

            // go to order if the first from shipment is delivered
            // go to customer in any case
            // TODO define a better way to find the respective order actor. the order id must map to the actor
            // shipments[shipment_id].in
            IOrderActor orderActor = null; // = GrainFactory.GetGrain<IOrderActor>(invoice.orderActorId);
            var custActor = shipments[shipment_id].customer_id % nCustPartitions;
            if (countDelivered == 0)
            {
                // await orderActor.UpdateOrderStatus(invoice.order.order_id, OrderStatus.SHIPPED);
                await GrainFactory.GetGrain<ICustomerActor>(custActor).NotifyDelivery(shipments[shipment_id].customer_id);
            } else
            {
                await orderActor.noOp();
                await GrainFactory.GetGrain<ICustomerActor>(custActor).NotifyDelivery(shipments[shipment_id].customer_id);

                if (shipments[shipment_id].package_count == countDelivered + 1)
                {
                    // TODO create shipment status enum? look into olist api first
                    //shipments[shipment_id].status = 
                }
            }

            return;
        }

    }
}

