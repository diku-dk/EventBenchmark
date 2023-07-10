namespace Common.Requests
{
    public record UpdatePrice(long sellerId, long productId, decimal price, int instanceId);
}