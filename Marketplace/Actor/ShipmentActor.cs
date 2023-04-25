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
        private readonly Dictionary<long, List<Package>> packages; // deliveries that are prescribed in the benchmark

        private int nCustPartitions;
        private int nOrderPartitions;

        private long shipmentActorId;

        private readonly Func<long, IList<Package>> queryPendingPackagesBySeller;

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
                id = this.nextShipmentId,
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
                    shipment_id = this.nextShipmentId,
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

            this.shipments.Add(shipment.id, shipment);
            this.packages.Add(shipment.id, packages_);

            this.nextShipmentId++;

            /**
             * Based on olist (https://dev.olist.com/docs/orders), the status of the order is
             * shipped when "at least one order item has been shipped"
             * All items are considered shipped here, so just signal the order about that
             */
            IOrderActor orderActor = GrainFactory.GetGrain<IOrderActor>(invoice.order.id % nOrderPartitions);
            await orderActor.UpdateOrderStatus(invoice.order.id, OrderStatus.SHIPPED);

            // TODO notify customer about shipment!

            this._logger.LogWarning("Shipment grain {0} -- Shipment process finished for order {0}", this.shipmentActorId, invoice.order.id);
        }

        /**
         * Inspired by: https://dev.olist.com/docs/retrieving-packages-informations
         * Index-based operation.
         * Can be probably used in the GetOverview transaction in seller
         */
        public Task<IList<Package>> GetPendingPackagesBySeller(long seller_id)
        {
            return Task.FromResult(this.queryPendingPackagesBySeller.Invoke(seller_id));            
        }

        /**
         * Inspired by Delivery Update from TPC-C
         * But adapted to a microservice application 
         * (i.e., writes to customer and order become events emitted)
         * and olist business rules.
         * Seller kind of substitutes district as the block of iteration.
         * Big transaction, run in batch. Touch many microservices, all-or-nothing exactly once guarantee.
         * Changes must be reflected in all microservices.
         * A set of deliveries to be updated as a transaction. Producing multiple messages.
         * Think about the trransactional guarantees we need.
         */
        public async Task UpdateShipment() // seller id
        {
            this._logger.LogWarning("Shipment grain {0} -- Update Shipment starting", this.shipmentActorId);

            /**
             * get the oldest (OPEN) shipment per seller
             * select seller id, min(shipment id)
             * from packages 
             * where packages.status == shipped
             * group by seller id
             */
            // generating the delivered packages. producing the deliveries here
            var q = this.packages.SelectMany(x => x.Value)
                                .Where(x => x.status.Equals(PackageStatus.shipped.ToString()))
                                .GroupBy(x => x.seller_id)
                                .Select(g => new { key = g.Key, Sort = g.Min( x => x.shipment_id ) } )
                                .ToDictionary(g => g.key, g => g.Sort);

            this._logger.LogWarning("Shipment grain {0} -- Shipments to update: \n{1}", this.shipmentActorId, string.Join(Environment.NewLine, q));

            /*
             *  for each seller, get the oldest shipment
             *    [13, 1] -> seller id , shipment id
                  [11, 1]
                  [14, 2]
                  [4, 3]
                  [2, 3]
             * 
             */

            List<Task> tasks = new();
            foreach (var kv in q)
            {
                // check if that avoids reentrancy from calling update package twice for same package. if not, disable reentrancy for this method

                // get all the packages of this seller in this shipment
                var packages_ = packages[kv.Value].Where(p => p.seller_id == kv.Key && p.status.Equals(PackageStatus.shipped.ToString())).ToList();
                tasks.AddRange( UpdatePackageDelivery(packages_) );
            }

            await Task.WhenAll(tasks);

            this._logger.LogWarning("Shipment grain {0} -- Update Shipment finished", this.shipmentActorId);

        }

        /**
         * Business rules. Should be specified in the benchmark.
         * (i) order actor is notified only if all deliveries are done
         * (ii) customer is notified about every delivery
         * (iii) shipment status change to concluded when all deliveries are concluded
         */
        private List<Task> UpdatePackageDelivery(List<Package> sellerPackages)
        {
            long shipment_id = sellerPackages.ElementAt(0).shipment_id;
            long seller_id = sellerPackages.ElementAt(0).seller_id;

            // aggregate operation
            int countDelivered = this.packages[shipment_id].Where(p => p.status == PackageStatus.delivered.ToString()).Count();

            this._logger.LogWarning("Shipment grain {0} -- Count delivery for shipment id {1}: {2} total of {3}",
                this.shipmentActorId, shipment_id, countDelivered, this.shipments[shipment_id].package_count);

            // if falls in this exception, implementation must be rethought
            // if (package.status.Equals(PackageStatus.delivered.ToString())) throw new Exception("Package is already delivered");

            // update
            foreach (var package in sellerPackages)
            {
                package.status = PackageStatus.delivered.ToString();
            }
              
            IOrderActor orderActor = GrainFactory.GetGrain<IOrderActor>(this.shipments[shipment_id].order_id % nOrderPartitions);
            var custActor = this.shipments[shipment_id].customer_id % nCustPartitions;

            List<Task> tasks = new(2)
            {
                GrainFactory.GetGrain<ICustomerActor>(custActor).NotifyDelivery(this.shipments[shipment_id].customer_id)
            };

            if (this.shipments[shipment_id].package_count == countDelivered + sellerPackages.Count())
            {
                this._logger.LogWarning("Shipment grain {0} -- Delivery concluded for shipment id {1}", this.shipmentActorId, shipment_id);
                // send message to order in the first delivery
                tasks.Add(orderActor.UpdateOrderStatus(this.shipments[shipment_id].order_id, OrderStatus.DELIVERED));
                this.shipments[shipment_id].status = ShipmentStatus.concluded.ToString();
            } else
            {
                this._logger.LogWarning("Shipment grain {0} -- Delivery not concluded yet for shipment id {1}: count {2} of total {3}",
                    this.shipmentActorId, shipment_id, countDelivered + sellerPackages.Count(), this.shipments[shipment_id].package_count);
                // the need to send this message, one drawback of determinism 
                tasks.Add(orderActor.noOp());
            }

            // await Task.WhenAll(tasks);
            return tasks;
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

