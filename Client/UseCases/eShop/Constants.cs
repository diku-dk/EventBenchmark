using System;
namespace Client.UseCases.eShop
{
    public static class Constants
    {

        // every cart/basket must have at least 5 items?
        public const int MIN_NUM_ITEMS = 5;
        public const int MAX_NUM_ITEMS = 15;

        // every item in the cart must have at least 1 quantity
        public const int MIN_ITEM_QTY = 1;
        public const int MAX_ITEM_QTY = 10;

        public const int NUM_TOTAL_ITEMS = 100000;
        public const int DIST_PER_WARE = 10;
        public const int CUST_PER_DIST = 3000;
        public const int ORD_PER_DIST = 3000;

        public const int DEFAULT_NUM_WARE = 1; //8;

        // every product in stock must have at least 10 available items
        public const int MIN_STOCK_QTY = 10;
        public const int MAX_STOCK_QTY = 1000;

    }
}
