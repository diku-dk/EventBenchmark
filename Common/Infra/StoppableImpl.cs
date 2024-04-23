namespace Common.Infra
{
    public class StoppableImpl : IStoppable, IDisposable
    {

        private readonly CountdownEvent cde;

        public StoppableImpl()
        {
            this.cde = new CountdownEvent(1);
        }

        public void Stop()
        {
            _ = cde.Signal();
        }

        public bool IsRunning()
        {
            return !cde.IsSet;
        }

        public virtual void Dispose()
        {
            cde.Dispose();
        }
    }
}
