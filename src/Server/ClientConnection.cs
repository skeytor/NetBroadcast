using System.Net.Sockets;
using System.Threading.Channels;

namespace Server;

/// <summary>
/// Represents a client connection to the server. It encapsulates the TCP client and provides a channel for sending byte arrays to the client.
/// </summary>
/// <param name="tcp"></param>
internal sealed class ClientConnection(TcpClient tcp) : IDisposable
{
    // A bounded channel to hold byte arrays that need to be sent to the client
    private readonly Channel<byte[]> _channel = Channel.CreateBounded<byte[]>(capacity: 100);
    private readonly TcpClient _tcp = tcp;

    public Guid Id { get; } = Guid.NewGuid();
    public NetworkStream Stream { get; } = tcp.GetStream();
    
    /// <summary>
    /// Gets the reader end of the channel.
    /// </summary>
    public ChannelReader<byte[]> Reader => _channel.Reader;

    /// <summary>
    /// Attempts to enqueue a byte array to be sent to the client. 
    /// Returns true if the byte array was successfully enqueued, false otherwise.
    /// </summary>
    /// <param name="payload"></param>
    /// <returns></returns>
    public bool TryEnqueue(byte[] payload) => _channel.Writer.TryWrite(payload);

    /// <summary>
    /// Attempts to complete the channel, signaling that no more byte arrays will be enqueued.
    /// </summary>
    /// <returns></returns>
    public bool TryComplete() => _channel.Writer.TryComplete();

    public void Dispose()
    {
        _tcp.Close();
    }
}
