using System;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Common.Http;
using Newtonsoft.Json.Linq;

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

    public async Task<long> Run(CancellationToken token)
    {
        // get first tid
        this.firstTid = this.PollLastCommittedTid();
        Console.WriteLine($"Polling task starting with the options:\nURL: {urlCommitted}\nRate: {this.rate}\nFirst TID: {this.firstTid}");

        // this.Poll(token);
        await this.PollSse(token);
        
        var last = Interlocked.Read(ref this.lastTid);
        Console.WriteLine("Polling task exiting with last TID: "+last);
        return last - this.firstTid;
    }

    private async Task PollSse(CancellationToken token)
    {
        // Ensure we keep headers open for streaming
        HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, this.urlCommitted);
        request.Headers.Accept.Add(new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("text/event-stream"));
        try
        {
            using (HttpResponseMessage response = await HttpUtils.client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token))
            {
                response.EnsureSuccessStatusCode();
                using (StreamReader reader = new StreamReader(await response.Content.ReadAsStreamAsync(token)))
                {
                    string line;
                    while ((line = await reader.ReadLineAsync(token)) != null)
                    {
                        if (!string.IsNullOrEmpty(line))
                        {
                            // Process each event (ignoring "data: " prefix)
                            if (line.StartsWith("data:"))
                            {
                                string eventData = line.Substring(5).Trim();
                                Console.WriteLine($"Received TID: {eventData}");
                                Interlocked.Exchange(ref lastTid, long.Parse(eventData));
                            }
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error receiving SSE: {ex.Message}");
        }
    }

    private void Poll(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            long newTid;
            try
            {
                // start sleeping, very unlikely to get batch completed on first request
                Thread.Sleep(this.rate);
                newTid = this.PollLastCommittedTid();
                if (newTid > this.lastTid)
                {
                    this.lastTid = newTid;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }

    public long GetNumberOfExecutedTIDs()
    {
        return this.lastTid - this.firstTid;
    }

}


