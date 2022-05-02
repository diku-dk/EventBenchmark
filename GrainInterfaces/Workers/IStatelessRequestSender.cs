using Common;
using Common.Http;
using Orleans;
using System.Net.Http;
using System.Threading.Tasks;

namespace GrainInterfaces.Workers
{
    public interface IStatelessRequestSender : IGrainWithIntegerKey
    {

        Task<HttpResponseMessage> Send(HttpRequest request);
        // Task<int> Steal(int points);

    }
}
