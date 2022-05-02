namespace Common
{
    public sealed class PlayerUpdate
    {
        public readonly int playerId;

        public readonly int update;
        public PlayerUpdate(int playerId, int update) {
            this.playerId = playerId;
            this.update = update;
        }

    }
}
