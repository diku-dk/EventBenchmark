using System;
namespace Common.Scenario.Entity
{
	public class Seller
    {

        public long seller_id { get; set; }

        public string name { get; set; }

        public string company_name { get; set; }

        public string email { get; set; }

        public string phone { get; set; }

        public string mobile_phone { get; set; }

        public string cpf { get; set; }

        public string cnpj { get; set; }

        /*
        public string street1 { get; set; }

        public string street2 { get; set; }

        public string seller_zip_code_prefix { get; set; }

        public string seller_city { get; set; }

        public string seller_state { get; set; }

        public decimal tax { get; set; }

        public int ytd { get; set; }
        */

        public int order_count { get; set; }

    }
}

