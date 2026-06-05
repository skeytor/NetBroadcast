# NetBroadcast

A multi-client TCP broadcast chat application built with .NET 10 in order to demonstrate concurrent message handling using async/await patterns and <b> channel-based communication </b>.

## Key Approaches

### **Server-Side Architecture (Server Project)**

- **TcpListener with Connection Pooling**: Accepts multiple concurrent client connections on port 5000 and manages them in a `ConcurrentDictionary` for thread-safe access.

- **Client Connection Encapsulation**: Each client is wrapped in a `ClientConnection` object that encapsulates the TCP client, network stream, and a bounded `System.Threading.Channels.Channel<byte[]>` for non-blocking message queuing.

- **Dual-Task Pattern per Client**: 
  - **Reader Task** (`ReadClientStreamAsync`): Reads incoming bites from the client's network stream
  - **Writer Task** (`WriteClientStreamAsync`): Processes outgoing bites from the channel and writes them to the network stream

- **Broadcast Mechanism**: Bites received from one client are asynchronously enqueued to all other connected clients via `TryEnqueue()` (non-blocking).

- **Length-Prefixed Protocol**: Messages use a 4-byte big-endian header to indicate payload length, preventing framing issues and supporting arbitrary binary payloads up to 64 KB.

- **Graceful Shutdown**: Proper exception handling and resource cleanup ensure clients are disconnected and removed from the pool when connections fail.

#### Message Framing (Length-Prefixed Protocol)
```
[4-byte Length][Payload]
```
Example: A message "Hello" would be sent as:
```
[00 00 00 05][48 65 6C 6C 6F]
```
Where `00 00 00 05` is the big-endian representation of the length (5 bytes) and `48 65 6C 6C 6F` is the ASCII encoding of "Hello".

### **Client-Side Architecture (ClientUI Project)**

- **Concurrent I/O with Channels**: Uses a bounded channel to decouple reading from the server and processing/displaying messages to avoid backpressure.

- **Three-Task Coordination**:
  - **Read Task**: Reads from the network stream using the same length-prefixed protocol
  - **Write Task**: Reads from the console (blocking) and sends to the server
  - **Process Task**: Consumes messages from the channel and renders them with UI state preservation

- **Graceful Termination**: Supports Ctrl+C (twice) to cancel operations cleanly with proper resource disposal.

## Tools

- **Async/Await**: Fully asynchronous I/O with `ReadExactlyAsync`, `WriteAsync`, and channel iteration
- **System.Threading.Channels**: Bounded channels for producer-consumer message queuing to avoid blocking calls and backpressure
- **System.Buffers.Binary**: Binary serialization with `BinaryPrimitives` for big-endian encoding
- **ConcurrentDictionary**: Thread-safe client registry
- **TcpListener/TcpClient**: Low-level socket abstractions for TCP communication

## Message Flow

```
Client A ──> [Server] ──> All Clients Except A
              │
              └──> Client B
              └──> Client C
```

Each message is broadcasted to all connected clients except the sender, enabling real-time chat across multiple connections.
