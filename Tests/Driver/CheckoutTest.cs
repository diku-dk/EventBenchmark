using System.Collections.Concurrent;
using Common.Workload;

namespace Tests.Driver;

public class CheckoutTest
{

    private static int numThreads = 10000;
    private static int numCustomers = 10000;

    [Fact]
    public void TestDist()
    {
        int numCpu = Environment.ProcessorCount;
        Interval customerRange = new Interval(1,numCustomers);
        BlockingCollection<int>[] customerIdleQueues = new BlockingCollection<int>[Environment.ProcessorCount];
        for(int i = 0; i < Environment.ProcessorCount; i++)
        {
            customerIdleQueues[i] = new BlockingCollection<int>(new ConcurrentQueue<int>()); 
        }

        int maxPerPartition = customerRange.max / numCpu;
        int curr = 1;
        int currTotal = 1;
        for(int i = 0; i < numCpu; i++)
        {
            while ( customerIdleQueues[i].TryTake(out _) ){ }
            while (currTotal <= customerRange.max && curr <= maxPerPartition)
            {
                customerIdleQueues[i].Add(currTotal);
                curr++;
                currTotal++;
            }
            curr = 1;
        }

        Assert.True(true);

    }

	[Fact]
	public void TestCheckoutRatio()
	{
        Interval customerRange = new Interval(1,numCustomers);
        BlockingCollection<int> customerIdleQueue = new BlockingCollection<int>(numCustomers);

		Console.WriteLine("Setting up {0} customers.",customerRange.max);
        while ( customerIdleQueue.TryTake(out _) ){}
        for (int i = customerRange.min; i <= customerRange.max; i++)
        {
            customerIdleQueue.Add(i);
        }

        Dictionary<string,(int idInit,int idEnd)> dict = new Dictionary<string, (int idInit, int idEnd)>();

        List<Task> tasks = new(numThreads);

        ConcurrentQueue<string> errorQueue = new();

        for(int tid = 1; tid <= numThreads; tid++){
            int customerId = customerIdleQueue.Take();
            string myTid = tid.ToString();
            dict.Add(myTid,(customerId,0));
           
             _ =   Task.Run(() => DoNothing(myTid)).ContinueWith(async x =>
                {
                    string tid_ = await x;
                    customerIdleQueue.Add(customerId);
                    if(dict[tid_].idInit != customerId)
                    {
                        errorQueue.Enqueue(tid_);
                        Console.WriteLine("Error!");
                    }
                }); //.ConfigureAwait(true);
            
        }

        Assert.True(errorQueue.Count == 0);

	}

    public static string DoNothing(string tid)
    {
        return tid;
    }

}


