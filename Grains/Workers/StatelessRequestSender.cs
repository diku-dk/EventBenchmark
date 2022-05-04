using System.Threading.Tasks;
using Common;
using Common.Http;
using GrainInterfaces.Workers;
using Orleans;

using System.Net.Http;
using Orleans.Concurrency;

namespace Grains
{
    [StatelessWorker]
    public class StatelessRequestSender : Grain, IStatelessRequestSender
    {

        private readonly HttpClient client;


        // private readonly Dictionary<int, int> scoreboard;

        public StatelessRequestSender()
        {
            // scoreboard = new Dictionary<int, int>();
            this.client = new HttpClient();
        }

        public async override Task OnActivateAsync()
        {
            var streamProvider = GetStreamProvider("SMSProvider");
            var stream = streamProvider.GetStream<RequestResult>(Constants.playerUpdatesStreamId, Constants.streamNamespace);
            // await stream.SubscribeAsync(UpdatePlayerScoreAsync);
        }

        // TODO should also be responsible for building the http request, this is stateless stuff

        public async Task<HttpResponseMessage> Send(HttpRequest request)
        {

            var content = new FormUrlEncodedContent(request.payload);

            return await client.PostAsync(request.url, content);

    
        }

       


    }


}