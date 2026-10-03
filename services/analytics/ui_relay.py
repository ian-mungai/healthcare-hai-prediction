"""Forward the container's port 4214 to the DuckDB UI, which listens only on the container's loopback (port 4213).

Compose publishes 4214 on the host's 127.0.0.1 only, so the UI stays reachable from this Mac alone.
"""

from __future__ import annotations

import asyncio

# Host None listens on every interface of the container only; Compose publishes the port on the host loopback alone.
LISTEN_PORT = 4214
TARGET = ("localhost", 4213)


async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Copy bytes one way until the sender closes."""
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    finally:
        writer.close()


async def handle(client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter) -> None:
    """Connect one browser connection to the UI and copy both ways."""
    try:
        ui_reader, ui_writer = await asyncio.open_connection(*TARGET)
    except OSError:
        client_writer.close()
        return
    await asyncio.gather(pipe(client_reader, ui_writer), pipe(ui_reader, client_writer), return_exceptions=True)


async def main() -> None:
    """Serve until the container stops."""
    server = await asyncio.start_server(handle, None, LISTEN_PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
