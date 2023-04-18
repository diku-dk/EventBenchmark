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
using System.Text;
using Orleans.Concurrency;
using Marketplace.Interfaces;
using Microsoft.Extensions.Logging;

namespace Marketplace.Actor
{

    public class ShipmentActor : Grain, IShipmentActor
    {
        private long nextShipmentId;
        // PK
        private readonly Dictionary<long, Shipment> shipments;
        // other table of shipment
        private readonly Dictionary<long, List<Package>> packages;
        private long nCustPartitions;
        private long nOrderPartitions;
        private long shipmentActorId;

        private readonly Func<long, List<Package>> queryPendingPackagesBySeller;

        private readonly ILogger<ShipmentActor> _logger;

        public ShipmentActor(ILogger<ShipmentActor> _logger)
        {
            this.nextShipmentId = 1;
            this.shipments = new();
            this.packages = new();
            this._logger = _logger;
            this.queryPendingPackagesBySeller = (x) => packages.Values.SelectMany(p => p)
                .Where(p => p.status == PackageStatus.shipped.ToString() && p.seller_id == x).ToList();
        }

        public override async Task OnActivateAsync()
        {
            this.shipmentActorId = this.GetPrimaryKeyLong();
            var mgmt = GrainFactory.GetGrain<IMetadataGrain>(0);
            var dict = await mgmt.GetActorSettings(new List<string>() { "CustomerActor", "OrderActor" });
            this.nCustPartitions = dict["CustomerActor"];
            this.nOrderPartitions = dict["OrderActor"];
            this._logger.LogWarning("Shipment grain {0} activated: #customer grains {1} #order grains {2}", this.shipmentActorId, nCustPartitions, nOrderPartitions);
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
            this._logger.LogWarning("Order grain {0} -- Shipment process starting for order {0}", this.shipmentActorId, invoice.order.id);

            // aggregate per seller
            Dictionary<long, int> sellerDeliveryIdMap = new();
            int package_id = 1;

            // https://stackoverflow.com/questions/19517707/sort-list-based-on-group-count
            var items = invoice.items
                        .GroupBy(x => x.seller_id)
                        .OrderByDescending(g => g.Count())
                        .SelectMany(x => x).ToList();

            // create the shipment
            long today = DateTime.Now.Millisecond;
            Shipment shipment = new()
            {
                id = nextShipmentId,
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

            shipments.Add(shipment.id, shipment);
            packages.Add(shipment.id, packages_);

            nextShipmentId++;

            /**
             * Based on olist (https://dev.olist.com/docs/orders), the status of the order is
             * shipped when "at least one order item has been shipped"
             * All items are considered shipped here, so just signal the order about that
             */
            IOrderActor orderActor = GrainFactory.GetGrain<IOrderActor>(invoice.order.id % nOrderPartitions);
            await orderActor.UpdateOrderStatus(invoice.order.id, OrderStatus.SHIPPED);

            this._logger.LogWarning("Shipment grain {0} -- Shipment process finished for order {0}", this.shipmentActorId, invoice.order.id);

        }

        /**
         * Inspired by: https://dev.olist.com/docs/retrieving-packages-informations
         * Index-based operation.
         * Can be probably used in the GetOverview transaction in seller
         */
        public Task<List<Package>> GetPendingPackagesBySeller(long seller_id)
        {
            return Task.FromResult(queryPendingPackagesBySeller.Invoke(seller_id));            
        }

        /**
         * Inspired by Delivery Update from TPC-C
         * But adapted to a microservice application 
         * (i.e., writes to customer and order become events emitted)
         * and olist business rules.
         * Seller kind of substitutes district as the block of iteration
         */
        public async Task UpdateShipment()
        {
            this._logger.LogWarning("Shipment grain {0} -- Update Shipment starting", this.shipmentActorId);

            var q = packages.SelectMany(x => x.Value)
                                                .GroupBy(x => x.seller_id)
                                                .MinBy(y => y.Key)
                                                .Where(x => x.status.Equals(PackageStatus.shipped.ToString()))
                                                .ToDictionary(g => g.shipment_id, g => g.seller_id);

            List<Task> tasks = new(q.Count);
            foreach(var kv in q)
            {
                // check if that avoids reentrancy from calling update package twice for same package. if not, disable reentrancy for this method
                var packages_ = packages[kv.Key].Where(p => p.seller_id == kv.Value && p.status.Equals(PackageStatus.shipped.ToString()));
                foreach(var pack in packages_)
                {
                    tasks.Add(UpdatePackageDelivery(pack));
                }
            }

            await Task.WhenAll(tasks);

            this._logger.LogWarning("Shipment grain {0} -- Update Shipment finished", this.shipmentActorId);

            /*

            // mark this shipment, so the next delivery transaction will not touch this shipment
            shipments[oldestPendingShipment.id].status = ShipmentStatus.delivery_in_progress.ToString();

            var today = DateTime.Now.Millisecond;

            List<Package> packages_ = packages[oldestPendingShipment.id];
            foreach(var package in packages_)
            {
                package.status = PackageStatus.delivered.ToString();
                package.delivery_date = today;
            }

            IOrderActor orderActor = GrainFactory.GetGrain<IOrderActor>(shipments[oldestPendingShipment.id].order_id % nOrderPartitions);
            ICustomerActor customerActor = GrainFactory.GetGrain<ICustomerActor>(shipments[oldestPendingShipment.id].customer_id % nCustPartitions);
            List<Task> tasks = new(2)
            {
                customerActor.NotifyDelivery(shipments[oldestPendingShipment.id].customer_id),
                orderActor.UpdateOrderStatus(shipments[oldestPendingShipment.id].order_id, OrderStatus.DELIVERED)
            };

            shipments[oldestPendingShipment.id].status = ShipmentStatus.concluded.ToString();

            await Task.WhenAll(tasks);

            */
        }

        /**
         * 
         */
        private async Task UpdatePackageDelivery(Package package)
        {
            // aggregate operation
            var countDelivered = packages[package.shipment_id].Where(p => p.status == PackageStatus.delivered.ToString()).Count();

            // if falls in this exception, implementation must be rethought
            if (package.status.Equals(PackageStatus.delivered.ToString())) throw new Exception("Package is already delivered");

            package.status = PackageStatus.delivered.ToString();
              
            IOrderActor orderActor = GrainFactory.GetGrain<IOrderActor>(shipments[package.shipment_id].order_id % nOrderPartitions);
            var custActor = shipments[package.shipment_id].customer_id % nCustPartitions;

            List<Task> tasks = new(2)
            {
                GrainFactory.GetGrain<ICustomerActor>(custActor).NotifyDelivery(shipments[package.shipment_id].customer_id)
            };

            // since shipment is responsible for tracking individual packages
            // it is less complex (and less redundant) to make the shipment update the order status
            if (countDelivered == 0)
            {
                // send message to order in the first delivery
                tasks.Add(orderActor.UpdateOrderStatus(shipments[package.shipment_id].order_id, OrderStatus.DELIVERED));
            } else {
                // the need to send this message, one drawback of determinism 
                tasks.Add(orderActor.noOp());       
            }

            if (shipments[package.shipment_id].package_count == countDelivered + 1)
            {
                shipments[package.shipment_id].status = ShipmentStatus.concluded.ToString();
            }

            await Task.WhenAll(tasks);
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

    }
}

