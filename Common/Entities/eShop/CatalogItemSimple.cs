namespace Common.Entities.eShop
{
    /**
     * https://github.com/dotnet-architecture/eShopOnContainers/blob/dev/src/ApiGateways/Web.Bff.Shopping/aggregator/Models/CatalogItem.cs
     * 
     */
    public class CatalogItemSimple
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public decimal Price { get; set; }

        public string PictureUri { get; set; }
    }
}
