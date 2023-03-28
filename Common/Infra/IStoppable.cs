namespace Common.Infra
{
    public interface IStoppable
    {

        public void Stop();

        public bool IsRunning();

    }
}
