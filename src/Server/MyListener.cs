using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace Server;

/// <summary>
/// A simple TCP server that listens for incoming client connections, reads messages from clients, 
/// and broadcasts messages to all other connected clients concurrently.
/// </summary>
/// <param name="port"></param>
internal sealed class MyListener(int port)
{
    // The length of the header prefix that indicates the length of the payload
    private const int HeaderPrefixLength = 4;

    // The maximum allowed size for a message payload (64 KB in this case)
    private const int MaxMessageSize = 1024 * 64;
    private readonly TcpListener _listener = new(IPAddress.Loopback, port);

    // A thread-safe dictionary to keep track of connected clients, keyed by their unique IDs
    private readonly ConcurrentDictionary<Guid, ClientConnection> _clients = [];

    /// <summary>
    /// Starts the server and continuously listens for incoming client connections.
    /// </summary>
    /// <returns></returns>
    public async Task RunAsync()
    {
        try
        {
            _listener.Start(10);
            Console.WriteLine($"[server] Listening on port {port}...");

            while (true)
            {
                TcpClient tcp = await _listener.AcceptTcpClientAsync();
                _ = HandleClientAsync(tcp);
            }
        }
        finally
        {
            _listener.Stop();
            Console.WriteLine("[server] Server stopped");
        }
    }

    // Handles individual client connections by reading bytes from the client's stream
    private async Task HandleClientAsync(TcpClient tcp)
    {
        using ClientConnection client = new(tcp);

        _clients[client.Id] = client;
        Console.WriteLine($"[server] Client {client.Id} connected successfully!");

        try
        {
            await Task.WhenAll(ReadClientStreamAsync(client), WriteClientStreamAsync(client));
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[server] Unexpected error for client {client.Id}: {ex}");
        }
        finally
        {
            Console.WriteLine($"[server] Disconnecting client {client.Id}");
            client.TryComplete();
            DisconnectClient(client);
        }
    }

    // Reads bites from the client's stream
    private async Task ReadClientStreamAsync(ClientConnection sender)
    {
        // Buffer to hold the header prefix
        byte[] header = new byte[HeaderPrefixLength];
        NetworkStream stream = sender.Stream;

        try
        {
            while (true)
            {
                await stream.ReadExactlyAsync(header);

                // Read the length of the payload from the header prefix (big-endian format)
                int length = BinaryPrimitives.ReadInt32BigEndian(header);

                // Checks if the length is a negative value or greater than the MaxMessageSize
                // Prevents potential issues such as memory exhaustion or malformed messages
                if ((uint)length > MaxMessageSize)
                {
                    throw new InvalidOperationException($"[server] Invalid payload length: {length}");
                }

                // TODO: Consider using a buffer pool to reduce memory allocations
                byte[] buffer = new byte[length];

                await stream.ReadExactlyAsync(buffer);
                Console.WriteLine($"[server] Received from {sender.Id}: {buffer.Length} bytes");

                BroadcastPayload(sender.Id, buffer);
            }
        }
        catch (EndOfStreamException) { sender.TryComplete(); }
    }

    // Writes bytes to the client's stream by reading from the client's channel
    private async Task WriteClientStreamAsync(ClientConnection client)
    {
        NetworkStream stream = client.Stream;

        try
        {
            await foreach (byte[] payload in client.Reader.ReadAllAsync())
            {
                // Write header and payload in a single batch
                int length = HeaderPrefixLength + payload.Length;

                byte[] buffer = new byte[length];

                // Write length prefix in big-endian format at the start of the buffer
                // buffer.AsSpan(0, HeadPrefixLength) writes the 4-th positions of the array
                BinaryPrimitives.WriteInt32BigEndian(buffer.AsSpan(0, HeaderPrefixLength), payload.Length);

                // Copy the payload immediately after the header prefix
                payload.CopyTo(buffer.AsSpan(HeaderPrefixLength..));

                await stream.WriteAsync(buffer.AsMemory(0..length));
            }
        }
        catch (IOException ex)
        {
            Console.WriteLine($"[server] Writer I/O error for {client.Id}: {ex.Message}");
        }
        catch (ObjectDisposedException)
        {
            Console.WriteLine($"[server] Writer stopped for {client.Id}");
        }
    }

    // Broadcasts the payload by enqueuing the message in a non-blocking manner
    private void BroadcastPayload(Guid senderId, byte[] message)
    {
        foreach (var (id, client) in _clients)
        {
            if (id != senderId)
            {
                // Writes directly to the client's channel without blocking
                client.TryEnqueue(message);
            }
        }
    }

    // Cleans up resources by removing the client
    private void DisconnectClient(ClientConnection client)
    {
        if (_clients.TryRemove(client.Id, out _))
        {
            client.Dispose();
        }
    }
}