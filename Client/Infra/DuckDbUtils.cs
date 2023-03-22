using System;
using System.Data.Common;

namespace Client.Infra
{
	public sealed class DuckDbUtils
	{
        public static void PrintQueryResults(DbDataReader queryResult)
        {
            for (var index = 0; index < queryResult.FieldCount; index++)
            {
                var column = queryResult.GetName(index);
                Console.Write($"Column: {column} ");
                var type = queryResult.GetFieldType(index);
                Console.WriteLine($"Type: {type} ");

            }

            Console.WriteLine();

            while (queryResult.Read())
            {
                for (int ordinal = 0; ordinal < queryResult.FieldCount; ordinal++)
                {
                    var val = queryResult.GetValue(ordinal);
                    Console.Write(val);
                    Console.Write(" ");
                }

                Console.WriteLine();
            }
        }

    }
}

