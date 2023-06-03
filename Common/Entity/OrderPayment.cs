using System;
namespace Common.Entity
{
    public class OrderPayment
    {
        public long order_id { get; set; }

        // 1 - coupon, 2 - coupon, 3 - credit card
        public int payment_sequential { get; set; }

        // coupon, credit card
        public PaymentType payment_type { get; set; }

        // number of times the credit card is charged (usually once a month)
        public int payment_installments { get; set; }

        // respective to this line (ie. coupon)
        public decimal payment_value { get; set; }
    }
}

