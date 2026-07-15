# NetDriver

**TCP framing tool** — lightweight message-oriented wrapper over raw TCP sockets.

---

## Overview

NetDriver is a C# library that provides structured framing for TCP communication. It operates above the byte-stream layer, introducing a typed frame protocol with built-in request-response correlation, flow segmentation, and configurable delivery modes.

The library is designed for scenarios requiring reliable message exchange over TCP with minimal overhead — real-time services, microservice communication, and custom protocol implementations.

**Current active version: AE**

---

## Architecture

NetDriver AE follows a layered pipeline architecture:

```
┌─────────────────────────────────────────────────────────────┐
│                      Networker (API)                        │
│  Send() / Answer() / SendFile() — public interface          │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                   LogicProcessor                            │
│  ┌─────────────┐  ┌─────────────┐  ┌───────────────────┐    │
│  │ Executor A  │  │ Executor B  │  │ Executor C/D/E    │    │
│  │ (incoming)  │  │ (callbacks) │  │ (sending / I/O)   │    │
│  └─────────────┘  └─────────────┘  └───────────────────┘    │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│              FrameController (Input / Output)               │
│  ┌─────────────────────┐    ┌───────────────────────────┐   │
│  │ FrameControllerInput│    │ FrameControllerOutput     │   │
│  │  • simpleleOutput   │    │  • outcomingStack         │   │
│  │  • answersOnReq     │    │  • SendWithCallback()     │   │
│  │  • SystemSend       │    │  • SendSingle()           │   │
│  │  • Distribute()     │    │  • SendFile()             │   │
│  └─────────────────────┘    └───────────────────────────┘   │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                   Framer (Serialization)                    │
│  ┌───────────────┐  ┌─────────────────┐  ┌───────────────┐  │
│  │ FrameBuilder  │  │  FrameParser    │  │   netframe    │  │
│  │  PackHeader() │  │  UnpackHeader() │  │  Header       │  │
│  │  PackContent()│  │  UnpackContent()│  │  Content      │  │
│  │  PackFrame()  │  │  UnpackFrame()  │  │  Type enum    │  │
│  └───────────────┘  └─────────────────┘  └───────────────┘  │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                   Transport Layer                           │
│  ┌─────────────────────┐    ┌───────────────────────────┐   │
│  │ IncomingController  │    │ OutcomingController       │   │
│  │  • Pipe-based read  │    │  • Channel-based write    │   │
│  │  • GetChunk()       │    │  • Send()                 │   │
│  └─────────────────────┘    └───────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## Frame Protocol

Each transmitted unit is a structured frame with the following layout:

| Field | Size | Description |
|-------|------|-------------|
| `contentSize` | 4 bytes | Payload length (little-endian) |
| `type` | 1 byte | Frame type discriminator |
| `numInFlow` | 4 bytes | Sequence number for flow segmentation |
| `frameuid` | 16 bytes | GUID — correlation identifier |
| `content` | variable | Application payload |

### Frame Types

| Type | Value | Semantics |
|------|-------|-----------|
| `single` | 0 | Unidirectional message, no response expected |
| `callbackFrom` | 1 | Request expecting a correlated response |
| `callbackInto` | 2 | Response to a prior `callbackFrom` |
| `configurateFlow` | 3 | Flow configuration directive |
| `flowPart` | 4 | Segmented fragment of a larger transmission |

---

## Key Capabilities

**Request-response correlation** — `Send(true, payload)` transmits a `callbackFrom` frame with a unique GUID. The receiving side can respond via `Answer(payload, guid)`, and the originator receives the correlated `ResultContent`.

**File transfer** — `SendFile(path, param, partSize)` segments files into frames of type `flowPart`, with configurable chunk size (default 32 MB). Supports `Straight`, `Random`, and `Reverse` transmission modes.

**Async pipeline** — built on `System.IO.Pipelines` for efficient zero-copy receive buffering and `System.Threading.Channels` for non-blocking outgoing queues.

**Dispose pattern** — full `IAsyncDisposable` implementation with cooperative cancellation across all executor tasks.

---

## Performance Characteristics

| Metric | Value |
|--------|-------|
| Header overhead | 9 bytes fixed |
| Frame type discrimination | O(1) |
| Receive buffering | `ArrayPool.Shared` (8 KB default) |
| Outgoing queuing | Unbounded channel, async non-blocking |
| Concurrent executors | 5 parallel tasks (A–E) |

---

## Usage Example

```csharp
using NetDriver.AE;

// Establish socket connection
var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
await socket.ConnectAsync("127.0.0.1", 8080);

// Define incoming event handler
async Task OnIncoming(ResultContent result)
{
    switch (result.type)
    {
        case ResultContent.Type.single:
            Console.WriteLine($"Received: {Encoding.UTF8.GetString(result.content)}");
            break;
        case ResultContent.Type.from:
            // Respond to request
            await networker.Answer(Encoding.UTF8.GetBytes("ACK"), result.frameuid.Value);
            break;
    }
}

// Initialize networker
var networker = new Networker(socket, OnIncoming);

// Send a message (no response expected)
await networker.Send(false, Encoding.UTF8.GetBytes("Hello"));

// Send with callback (expects response)
var response = await networker.Send(true, Encoding.UTF8.GetBytes("Request"));
if (response != null)
    Console.WriteLine($"Response: {Encoding.UTF8.GetString(response.content)}");

// Cleanup
await networker.Dispose();
await socket.DisposeAsync();
```

---

## Versioning

| Version | Status |
|---------|--------|
| AC | Legacy |
| AD | Legacy |
| **AE** | **Active** |

---

## Requirements

- .NET 10.0
- C# 13.0
