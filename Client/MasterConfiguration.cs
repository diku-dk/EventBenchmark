using System;
using Orleans;

namespace Client
{
	public class MasterConfiguration
	{
        public IClusterClient orleansClient;

        public bool streamEnabled = false;


    }
}

