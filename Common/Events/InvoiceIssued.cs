using Common.Entities;
using Common.Requests;

namespace Common.Events;

public class InvoiceIssued
{
    public CustomerCheckout customer { get; set; }

    public int orderId { get; set; }

    public string invoiceNumber { get; set; }

    public DateTime issueDate { get; set; }

    public float totalInvoice { get; set; }

    public List<CartItem> items { get; set; }

    public string instanceId { get; set; }

    public InvoiceIssued(){}

    public InvoiceIssued(CustomerCheckout customer, int orderId, string invoiceNumber, DateTime issueDate, float totalInvoice, List<CartItem> items, string instanceId)
    {
        this.customer = customer;
        this.orderId = orderId;
        this.invoiceNumber = invoiceNumber;
        this.issueDate = issueDate;
        this.totalInvoice = totalInvoice;
        this.items = items;
        this.instanceId = instanceId;
    }
}