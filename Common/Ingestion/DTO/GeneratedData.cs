using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Common.Ingestion.DTO
{
    /*
     * Just a wrapper to avoid passing a dictionary around
     * classes
     * 
     */
    public class GeneratedData
    {
        public Dictionary<string, List<string>> tables;

    }
}
