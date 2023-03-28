using System.Threading;

namespace Common.Infra
{
    public class Stoppable : IStoppable, System.IDisposable
    {

        private readonly CountdownEvent cde;

        public Stoppable()
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
