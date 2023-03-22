using Common.Ingestion.DTO;
using DuckDB.NET.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Client.DataGeneration.Real
{
    /**
     * 
     */
    public sealed class RealDataGenerator
    {
        private readonly OlistDataSourceConfiguration config;

        public RealDataGenerator(OlistDataSourceConfiguration config)
        {
            this.config = config;
        }

        public void Generate()
        {

            // make sure all files exist first
            foreach(var entry in config.mapTableToFileName)
            {
                if(!Directory.Exists(config.filePath + "/" + entry.Value))
                {
                    throw new Exception("Cannot generate table "+ entry.Key);
                }
            }

            using var connection = new DuckDBConnection(config.connectionString);
            connection.Open();
            var command = connection.CreateCommand();
            var sb = new StringBuilder();
            foreach (var entry in config.mapTableToFileName)
            {
                sb.Append("CREATE TABLE ").Append(entry.Key).Append(" AS SELECT * FROM read_csv('")
                    .Append(config.filePath).Append('/').Append(entry.Value)
                    .Append("', header=true, delim=',', AUTO_DETECT=TRUE);");

                command.CommandText = sb.ToString();
                var executeNonQuery = command.ExecuteNonQuery();
                sb.Clear();
            }

            // generate stock table based on products
            // a high number of stock at first to make it less complicated

            // use rowid for referring to customers and link to orders table


            // create stock items based on the order items. pick number of items sold



        }

    }
}
