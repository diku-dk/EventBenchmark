using System;
using Common.Entity;
using System.Threading.Tasks;
using Orleans;
using System.Collections.Generic;
using Marketplace.Infra;
using Marketplace.Interfaces;
using Microsoft.Extensions.Logging;

namespace Marketplace.Actor
{

    public class StockActor : Grain, IStockActor
	{

        private readonly Dictionary<long, StockItem> items;
        private readonly ILogger<StockActor> _logger;
        private long partitionId;

        public StockActor(ILogger<StockActor> _logger)
		{
            this.items = new();
            this._logger = _logger;
        }

        public override async Task OnActivateAsync()
        {
            this.partitionId = this.GetPrimaryKeyLong();
            // to avoid warning...
            await base.OnActivateAsync();
        }

        public Task DeleteItem(long productId)
        {
            this.items[productId].updated_at = DateTime.Now;
            this.items[productId].active = false;
            return Task.CompletedTask;
        }

        // called by order actor only
        public Task<ItemStatus> AttemptReservation(long productId, int quantity)
        {
            if (!this.items[productId].active)
            {
                return Task.FromResult(ItemStatus.DELETED);

            }
            if (this.items[productId].qty_available - this.items[productId].qty_reserved >= quantity)
            {
                this.items[productId].qty_reserved += quantity;
                this.items[productId].updated_at = DateTime.Now;
                return Task.FromResult(ItemStatus.IN_STOCK);
            }

            return Task.FromResult(ItemStatus.OUT_OF_STOCK);
        }

        // called by order actor only
        // deduct from stock reservation
        public Task ConfirmReservation(long productId, int quantity)
        {
            // deduct from stock
            this.items[productId].qty_available -= quantity;
            this.items[productId].qty_reserved -= quantity;
            this.items[productId].updated_at = DateTime.Now;
            return Task.CompletedTask;
        }

        // called by payment and order actors only
        public Task CancelReservation(long productId, int quantity)
        {
            // return item to stock
            this.items[productId].qty_reserved -= quantity;
            this.items[productId].updated_at = DateTime.Now;
            return Task.CompletedTask;
        }

        // called by payment actor only.
        // deduct from stock available
        public Task ConfirmOrder(long productId, int quantity)
        {
            // increase order count
            this.items[productId].order_count += 1;
            this.items[productId].updated_at = DateTime.Now;
            return Task.CompletedTask;
        }

        public Task AddItem(StockItem item)
        {
            this._logger.LogWarning("Stock part {0}, adding product ID {1}", this.partitionId, item.product_id);
            this.items.Add(item.product_id, item);
            this.items[item.product_id].created_at = DateTime.Now;
            return Task.CompletedTask;
        }

        Func<(ItemStatus, ItemStatus)> out_to_in = () => (ItemStatus.OUT_OF_STOCK, ItemStatus.IN_STOCK);
        Func<(ItemStatus, ItemStatus)> in_to_in = () => (ItemStatus.IN_STOCK, ItemStatus.IN_STOCK);
        // Func<(ItemStatus, ItemStatus)> del = () => (ItemStatus.DELETED, ItemStatus.DELETED);

        /**
         * Returns a derived transition
         */
        public Task<(ItemStatus,ItemStatus)> IncreaseStock(long productId, int quantity)
        {
            
            this.items[productId].qty_available += quantity;
            this.items[productId].updated_at = DateTime.Now;
            if (this.items[productId].qty_available == quantity)
            {
                return Task.FromResult(out_to_in.Invoke());
            }
            
            return Task.FromResult(in_to_in.Invoke());
        }

        public Task<StockItem> GetItem(long itemId)
        {
            return Task.FromResult(items[itemId]);
        }
    }
}

