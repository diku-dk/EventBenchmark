using Common.Workload;

namespace Monitor;

public sealed class CartActorConfig
{
    public Interval minMaxNumItemsRange { get; set; }

    // probability of a customer to checkout the cart
    public int checkoutProbability { get; set; }

    public string cartUrl { get; set; }

    public string checkoutUrl { get; set; }

    public Interval minMaxQtyRange { get; set; }

    public Interval delayBetweenRequestsRange { get; set; }

    public int voucherProbability { get; set; }

    public CartActorConfig(){ }

}
