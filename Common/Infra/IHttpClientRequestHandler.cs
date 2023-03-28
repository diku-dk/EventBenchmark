using System;
using System.Net;

namespace Common.Infra
{
	public interface IHttpClientRequestHandler
	{
        public void Handle(HttpListenerContext ctx);
    }
}

