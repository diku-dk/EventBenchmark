namespace Orleans.Infra;

public sealed class CustomHttpClientFactory : IHttpClientFactory
{
	public CustomHttpClientFactory()
	{
	}

    static SocketsHttpHandler handler = new SocketsHttpHandler
    {
        UseProxy = false,
        Proxy = null,
        UseCookies = false,
        AllowAutoRedirect = false,
        PreAuthenticate = false,
    };
    static HttpClient sharedClient = new HttpClient(handler);

    public HttpClient CreateClient(string name)
    {
        return sharedClient;
    }
}


