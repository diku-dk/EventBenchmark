namespace Tests.Thread;


public class CustomerTests
{
	// let A and B represent products of the same seller
	// let the order of updates on a product be represented as A1, A2, A3, ..., An (same for B)
	// considering cart added product B2
	// the cart cannot add B1 afterwards since it has already "seen" B2
	// adding a product to a cart has to indicate a version
	// cart with causal cut is only necessary if we had multi-object updates

    [Fact]
	public async void CartCheckTest()
	{

	}
}

