﻿using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;

namespace GrainInterfaces.Workers
{

    public interface IEventDispatcher : IGrainWithIntegerKey
    {


    }
}
