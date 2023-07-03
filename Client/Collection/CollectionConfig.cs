using System.Collections.Generic;

namespace Client.Collection
{
	public class CollectionConfig
	{
        public string baseUrl { get; set; }
        public string ready { get; set; }

        public string egress_count { get; set; }
        public List<AppIdTopic> egress_topics { get; set; }

        public string ingress_count { get; set; }
        public List<AppIdTopic> ingress_topics { get; set; }
    }
}

