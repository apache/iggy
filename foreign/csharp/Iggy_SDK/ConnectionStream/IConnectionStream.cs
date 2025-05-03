namespace Iggy_SDK.ConnectionStream;

public interface IConnectionStream : IDisposable
{
    public ValueTask SendAsync(ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default);
    public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default);
    public Task FlushAsync(CancellationToken cancellationToken = default);
    public void Close();
}