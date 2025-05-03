using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Exceptions;
using System.Net;
using System.Text;
using System.Text.Json;
using JsonConverterFactory = Iggy_SDK.JsonConfiguration.JsonConverterFactory;

namespace Iggy_SDK.MessagesDispatcher;

internal sealed class HttpMessageInvoker : IMessageInvoker
{
    private readonly HttpClient _client;

    public HttpMessageInvoker(HttpClient client)
    {
        _client = client;
    }
    public async Task SendMessagesAsync(MessageSendRequest request, CancellationToken token = default)
    {
        var json = JsonSerializer.Serialize(request, JsonConverterFactory.MessagesOptions);
        var data = new StringContent(json, Encoding.UTF8, "application/json");

        var response = await _client.PostAsync($"/streams/{request.StreamId}/topics/{request.TopicId}/messages", data, token);
        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }
    private static async Task HandleResponseAsync(HttpResponseMessage response)
    {
        if ((int)response.StatusCode > 300 && (int)response.StatusCode < 500)
        {
            var err = await response.Content.ReadAsStringAsync();
            throw new InvalidResponseException(err);
        }
        if (response.StatusCode == HttpStatusCode.InternalServerError)
        {
            throw new Exception("HTTP Internal server error");
        }
        throw new Exception("Unknown error occurred.");
    }
}