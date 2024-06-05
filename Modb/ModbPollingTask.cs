using System;
using System.Net.Http;
using System.Threading;
using Common.Http;

namespace Modb;

public sealed class ModbPollingTask
{
    private readonly string url;
    private readonly int rate;
    private int firstTid;
    private int lastTid;

	public ModbPollingTask(string pollingUrl, int pollingRate)
	{
        this.url = pollingUrl;
        this.rate = pollingRate;
	}

    private int PollTid()
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

        return (int) BitConverter.ToInt64(ba);
    }

    public int Run(CancellationToken token)
    {
        // get first tid
        this.firstTid = this.PollTid();
        Console.WriteLine($"Polling task starting with url {url}, rate {this.rate} and first tid as {this.firstTid}");
        
        int newTid;
        while (!token.IsCancellationRequested)
        {
            // Thread.Sleep(this.batchWindow);
            // Console.WriteLine($"Polling task sending request...");

            try{

                // start sleeping, very unlikely to get batch completed on first request
                Thread.Sleep(this.rate);

                newTid = this.PollTid();

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

    public int GetNumberOfExecutedTIDs()
    {
        return this.lastTid - this.firstTid;
    }

}


