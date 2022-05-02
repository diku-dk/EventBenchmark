namespace Common
{
    public class Token
    {
        public int lastPlayerID { get; set; }

        public int rounds;
        public Token(int firstPlayerID) {
            rounds = 0;
            lastPlayerID = firstPlayerID;
        }

    }
}
