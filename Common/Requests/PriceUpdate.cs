namespace Common.Requests;

public record PriceUpdate(int sellerId, int productId, float price, string version, string instanceId);