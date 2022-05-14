using System.Threading;

namespace Client.Infra
{
    public class Stoppable
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

        public bool IsStopped()
        {
            return cde.IsSet;
        }

    }
}
