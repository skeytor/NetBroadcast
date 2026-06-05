using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;

using TcpClient client = new();

// A channel to hold incoming messages from the server for processing
Channel<byte[]> messageQueue = Channel.CreateBounded<byte[]>(capacity: 100);
CancellationTokenSource cts = new();

// Press Ctrl+C twice to exit the chat client gracefully
Console.CancelKeyPress += (s, e) =>
{
    e.Cancel = true; // Prevent the process from terminating immediately
    cts.Cancel();
};

await client.ConnectAsync(IPAddress.Loopback, 5000);

await using NetworkStream stream = client.GetStream();

await InitializeAsync();

/// <summary>
/// Initializes the chat client
/// </summary>
async Task InitializeAsync()
{
    try
    {
        Console.WriteLine("Connected to the server. You can start chatting!\n");

        // Start the task for writing in background because the console's input is blocking
        Task writeTask = Task.Run(() => WriteStreamBitesAsync(cts.Token));

        // Start the tasks for reading and writing to the stream
        Task readTask = ReadStreamBitesAsync(cts.Token);
        Task processTask = ProcessMessagesAsync(cts.Token);

        await Task.WhenAll(readTask, processTask, writeTask);

    }
    catch (OperationCanceledException)
    {
        Console.WriteLine("Chat session ended.");
    }
    finally
    {
        messageQueue.Writer.TryComplete();
        client.Close();
        Console.WriteLine("Disconnected from the server.");
    }
}

async Task WriteStreamBitesAsync(CancellationToken ct)
{
    byte[] header = new byte[4];

    while (!ct.IsCancellationRequested)
    {
        Console.Write("You: ");
        string? message = Console.ReadLine();
        if (string.IsNullOrEmpty(message))
        {
            continue;
        }

        byte[] payload = Encoding.UTF8.GetBytes(message);

        // Write the length of the message in big-endian format at the start of the header
        BinaryPrimitives.WriteInt32BigEndian(header, payload.Length);

        // First write the header and then the payload
        await stream.WriteAsync(header.AsMemory(), ct);
        await stream.WriteAsync(payload.AsMemory(), ct);
    }
}

async Task ReadStreamBitesAsync(CancellationToken ct)
{
    byte[] header = new byte[4];

    while (!ct.IsCancellationRequested)
    {
        await stream.ReadExactlyAsync(header.AsMemory(), ct);

        int length = BinaryPrimitives.ReadInt32BigEndian(header);

        // TODO: Consider using a buffer pool to reduce memory allocations
        byte[] buffer = new byte[length];

        await stream.ReadExactlyAsync(buffer.AsMemory(), ct);

        // Enqueue the message for processing
        await messageQueue.Writer.WriteAsync(buffer, ct);
    }
}

async Task ProcessMessagesAsync(CancellationToken ct)
{
    string message = string.Empty;

    // Process messages as they arrive
    await foreach (byte[] bytes in messageQueue.Reader.ReadAllAsync(ct))
    {
        message = Encoding.UTF8.GetString(bytes);
        RenderMessageFrame(message);
    }
}

static void RenderMessageFrame(string message)
{
    // Save the current cursor position to restore it after rendering the message.
    int currentLeft = Console.CursorLeft;
    int currentTop = Console.CursorTop;

    // Move to line start and clear it.
    Console.SetCursorPosition(0, Math.Max(currentTop, 0));
    Console.Write(' ');
    Console.SetCursorPosition(0, Math.Max(currentTop, 0));
    Console.WriteLine($"Remote: {message}");
    Console.Write("You: ");
}