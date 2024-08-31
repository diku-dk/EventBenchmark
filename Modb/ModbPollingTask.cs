using System;
using System.Net.Http;
using System.Threading;
using Common.Http;

namespace Modb;

public sealed class ModbPollingTask
{
    private readonly string url;
    private readonly int rate;
    private long firstTid;
    private long lastTid;

	public ModbPollingTask(string pollingUrl, int pollingRate)
	{
        this.url = pollingUrl + "/status/committed";
        this.rate = pollingRate;
	}

    public long PollLastCommittedTid()
    {
        HttpResponseMessage response = HttpUtils.client.Send(new(HttpMethod.Get, url));  
        // Console.WriteLine("New result!!!");
        // StreamReader stream = new StreamReader(response.Content.ReadAsStream());
        if(!response.IsSuccessStatusCode)
        {
            Console.WriteLine("No sucess result!!!");
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
        Console.WriteLine($"Polling task starting with url {url}, rate {this.rate} and first tid as {this.firstTid}");
        long newTid;
        while (!token.IsCancellationRequested)
        {
            // Thread.Sleep(this.batchWindow);
            // Console.WriteLine($"Polling task sending request...");

            try{

                // start sleeping, very unlikely to get batch completed on first request
                Thread.Sleep(this.rate);

                newTid = this.PollLastCommittedTid();

                if (newTid > this.lastTid)
                {
                    this.lastTid = newTid;
                }

                /*
                int idx = init;
                while (idx <= lastTid)
                {
                    // always return true, it is an unbounded channel
                    Shared.ResultQueue.Writer.TryWrite(Shared.ITEM);
                    idx++;
                }
                */

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


