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
using System;
using System.Net;
using System.Net.Sockets;
using AVcontrol;
using NetDriver.AE;

namespace QA
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var a = new Side();
            var b = new Side();

            await Task.WhenAll([
                b.Init(Side.Role.ServerSide),
                a.Init(Side.Role.ClientSide),
            ]);

            await a.networker.Send(false, [1, 2, 3, 1, 3]);
            await b.networker.Send(false, [1, 2, 3, 1, 3]);

            await a.networker.SendFile("/home/nyashka/.config/wallpapers/HONjgUobgAAnUqL.jpg", FileParametrs.Random, 1024 * 16);

            Console.ReadKey();

            await a.DisposeAsync();
            await b.DisposeAsync();
        }
    }

    internal class Side : IAsyncDisposable
    {
        private readonly CancellationTokenSource _cts = new();
        private readonly Socket _socket;
        public Networker networker;
        public Side()
        {
            _socket = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            
            Console.Write("Start!\n");
        }

        public async Task Init(Role role)
        {
            switch(role)
            {
                case Role.ClientSide:
                    await _socket.ConnectAsync(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 22333), _cts.Token);
                    networker = new(_socket, IncomingEvent, DisconnectEvent);
                    break;
                case Role.ServerSide:
                    _socket.Bind(new IPEndPoint(IPAddress.Any, 22333));
                    _socket.Listen();
                    networker = new(await _socket.AcceptAsync(_cts.Token), IncomingEvent, DisconnectEvent);
                    break;
            }
        }

        private async Task IncomingEvent(ResultContent result)
        {
            Console.Write("че то поймал\n");
        }

        private async void DisconnectEvent(Socket sock)
        {
            await DisposeAsync();
        }

        public async ValueTask DisposeAsync()
        {
            _cts.Cancel();

            _socket.Disconnect(false);
            _socket.Close();
            _socket.Dispose();
            

            await networker.Dispose();

            _cts.Dispose();
        }

        public enum Role
        {
            ServerSide,
            ClientSide,
        }
    }
}
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
