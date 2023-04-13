using System;
using System.Net;

namespace Common.Infra
{
	public interface IHttpClientRequestHandler
	{
        /**
         * Async is added to the method implementation.
         * It is a client-based configuration.
         * The HttpServer is oblivious to how clients
         * process the incoming requests.
         */
        public void Handle(HttpListenerContext ctx);
    }
}

