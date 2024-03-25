using Common.Http;

namespace Statefun.Infra;

/**
* For StateFun only
*   
*/
public sealed class StatefunUtils
{

    public const string BASE_CONTENT_TYPE = "application/vnd.marketplace/";

    /**
     * used to send http request to StateFun application.
     */
    public static async Task<HttpResponseMessage> SendHttpToStatefun(HttpClient httpClient, string url, string contentType, string payLoad)
    {
        var content = HttpUtils.BuildPayload(payLoad);
        content.Headers.ContentType = null; // zero out default content type
        content.Headers.TryAddWithoutValidation("Content-Type", contentType);
        return await httpClient.PostAsync(url, content);
    }

}
