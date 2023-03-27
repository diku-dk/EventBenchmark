using System;
using Newtonsoft.Json;

namespace Common.Serdes
{
	public sealed class SerdesFactory
	{
		public static ISerdes build()
		{
            return new NewtonSerdes();
		}

        private class NewtonSerdes : ISerdes
        {
            public object Deserialize(string value)
            {
                return JsonConvert.DeserializeObject(value);
            }

            public T Deserialize<T>(string value)
            {
                return JsonConvert.DeserializeObject<T>(value);
            }

            public string Serialize(object value)
            {
                return JsonConvert.SerializeObject(value);
            }
        }

    }
}

