using System;
using Common.Entity;
using Marketplace.Infra;
using Orleans;
using System.Threading.Tasks;

namespace Marketplace.Interfaces
{
    public interface IStockActor : IGrainWithIntegerKey, SnapperActor
    {
        public Task DeleteItem(long productId);
        public Task<ItemStatus> AttemptReservation(long productId, int quantity);
        public Task CancelReservation(long productId, int quantity);
        public Task ConfirmReservation(long productId, int quantity);
        public Task ConfirmOrder(long productId, int quantity);

        // from seller
        public Task<(ItemStatus, ItemStatus)> IncreaseStock(long productId, int quantity);

        // API
        public Task AddItem(StockItem item);
    }
}

