namespace Common.Requests
{
    public record DeleteProduct(long sellerId, long productId, int instanceId);
}