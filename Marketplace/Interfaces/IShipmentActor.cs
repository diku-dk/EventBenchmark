using System;
using Common.Entity;
using Marketplace.Infra;
using Marketplace.Message;
using Orleans;
using Orleans.Concurrency;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Marketplace.Interfaces
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

        [AlwaysInterleave]
        public Task ProcessShipment(Invoice invoice);

        [AlwaysInterleave]
        public Task UpdateShipment();

        // retrieve the packages not delivered yet
        public Task<IList<Package>> GetPendingPackagesBySeller(long seller_id);
    }
}

