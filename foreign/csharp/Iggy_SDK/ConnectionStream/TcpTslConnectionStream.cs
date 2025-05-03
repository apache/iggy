using System.Net.Security;

namespace Iggy_SDK.ConnectionStream;

public sealed class TcpTlsConnectionStream : IConnectionStream
{
    private readonly SslStream _stream;

    public TcpTlsConnectionStream(SslStream stream)
    {
        _stream = stream;
    }
    public ValueTask SendAsync(ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default) 
        => _stream.WriteAsync(payload, cancellationToken);

    public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        => _stream.ReadAsync(buffer, cancellationToken);

    public Task FlushAsync(CancellationToken cancellationToken = default)
        => _stream.FlushAsync(cancellationToken);

    public void Close() 
        => _stream.Close();

    public void Dispose()
        => _stream.Dispose();
}