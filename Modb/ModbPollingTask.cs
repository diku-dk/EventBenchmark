using System;
using System.Net.Http;
using System.Threading;
using Common.Http;

namespace Modb;

public sealed class ModbPollingTask
{
    private readonly string urlCommitted;
    private readonly string urlSubmitted;
    private readonly int rate;
    private long firstTid;
    private long lastTid;

	public ModbPollingTask(string pollingUrl, int pollingRate)
	{
        this.urlCommitted = pollingUrl + "/status/committed";
        this.urlSubmitted = pollingUrl + "/status/submitted";
        this.rate = pollingRate;
	}

    public long PollLastSubmittedTid()
    {
        var request = new HttpRequestMessage(HttpMethod.Get, this.urlSubmitted);
        request.Headers.Add("Accept","application/octet-stream");
        HttpResponseMessage response = HttpUtils.client.Send(request);
        if(!response.IsSuccessStatusCode)
        {
            Console.WriteLine("Request for last submitted TID failed: "+response.ReasonPhrase);
            return -1;
        }
        byte[] ba = response.Content.ReadAsByteArrayAsync().Result;
        return BitConverter.ToInt64(ba);
    }

    public long PollLastCommittedTid()
    {
        var request = new HttpRequestMessage(HttpMethod.Get, this.urlCommitted);
        request.Headers.Add("Accept","application/octet-stream");

        HttpResponseMessage response = HttpUtils.client.Send(request);
        if(!response.IsSuccessStatusCode)
        {
            Console.WriteLine("Request for last committed TID failed: "+response.ReasonPhrase);
            Thread.Sleep(this.rate);
            return 0;
        }
        byte[] ba = response.Content.ReadAsByteArrayAsync().Result;
        return BitConverter.ToInt64(ba);
    }

    public long Run(CancellationToken token)
    {
        // get first tid
        this.firstTid = this.PollLastCommittedTid();
        Console.WriteLine($"Polling task starting with url {urlCommitted}, rate {this.rate} and first tid as {this.firstTid}");
        long newTid;
        while (!token.IsCancellationRequested)
        {
            try{
                // start sleeping, very unlikely to get batch completed on first request
                Thread.Sleep(this.rate);
                newTid = this.PollLastCommittedTid();
                if (newTid > this.lastTid)
                {
                    this.lastTid = newTid;
                }
            } catch(Exception e)
            {
                Console.WriteLine(e);
            }
        }
        Console.WriteLine("Polling task exiting...");
        return this.lastTid - this.firstTid;
    }

    public long GetNumberOfExecutedTIDs()
    {
        return this.lastTid - this.firstTid;
    }

}


