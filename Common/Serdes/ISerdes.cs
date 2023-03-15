using System;
namespace Common.Serdes
{
	public interface ISerdes
	{

        public string Serialize(object value);

        public object Deserialize(string value);

        public T Deserialize<T>(string value);

    }
}

