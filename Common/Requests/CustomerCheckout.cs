namespace Common.Requests;

/**
    * A sub-type of customer.
    * Ideally; address and credit card info may change across customer checkouts
    * Basket and Order does not need to know all public internal data about customers
    */
public class CustomerCheckout {

    public int CustomerId { get; set; }

    /**
    * Delivery address (could be different from customer's address)
    */
    public string FirstName { get; set; }

    public string LastName { get; set; }

    public string Street { get; set; }

    public string Complement { get; set; }

    public string City { get; set; }

    public string State { get; set; }

    public string ZipCode { get; set; }

    /**
    * Payment type
    */
    public string PaymentType { get; set; }

    /**
    * Credit or debit card
    */
    public string CardNumber { get; set; }

    public string CardHolderName { get; set; }

    public string CardExpiration { get; set; }

    public string CardSecurityNumber { get; set; }

    public string CardBrand { get; set; }

    // if no credit card, must be 1
    public int Installments { get; set; }

    public string instanceId { get; set; }

    public CustomerCheckout(){ }

    public CustomerCheckout(int customerId, string firstName, string lastName, string street, string complement, string city, string state, string zipCode, string paymentType, string cardNumber, string cardHolderName, string cardExpiration, string cardSecurityNumber, string cardBrand, int installments, string instanceId)
    {
        CustomerId = customerId;
        FirstName = firstName;
        LastName = lastName;
        Street = street;
        Complement = complement;
        City = city;
        State = state;
        ZipCode = zipCode;
        PaymentType = paymentType;
        CardNumber = cardNumber;
        CardHolderName = cardHolderName;
        CardExpiration = cardExpiration;
        CardSecurityNumber = cardSecurityNumber;
        CardBrand = cardBrand;
        Installments = installments;
        this.instanceId = instanceId;
    }
}