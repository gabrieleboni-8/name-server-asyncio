import asyncio
import json
from datetime import datetime

HOST = "127.0.0.1"
NS_PORT = 6000
TIME_PORT = 6001
ECHO_PORT = 6002

# Registro dei servizi: {nome: (host, porta)}
SERVICES = {
    "time": (HOST, TIME_PORT),
    "echo": (HOST, ECHO_PORT),
}


def error(message):
    return {"status": "error", "message": message}


async def send(writer, obj):
    """Scrive un oggetto JSON su una riga terminata da \\n."""
    writer.write((json.dumps(obj) + "\n").encode())
    await writer.drain()


async def serve_client(reader, writer, handler, name):
    """Ciclo comune: legge righe JSON, chiama handler(req) -> risposta."""
    peer = writer.get_extra_info("peername")
    print(f"[{name}] connesso {peer}")
    try:
        while True:
            line = await reader.readline()
            if not line:  # EOF: il client si è disconnesso
                break
            try:
                req = json.loads(line.decode())
            except (json.JSONDecodeError, UnicodeDecodeError):
                await send(writer, error("riga non è un JSON valido"))
                continue
            if not isinstance(req, dict):
                await send(writer, error("la richiesta deve essere un oggetto JSON"))
                continue
            await send(writer, handler(req))
    except (ConnectionResetError, BrokenPipeError):
        pass
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except (ConnectionResetError, BrokenPipeError):
            pass
        print(f"[{name}] disconnesso {peer}")



def handle_nameserver(req):
    cmd = req.get("cmd")
    if cmd != "lookup":
        return error(f"comando sconosciuto: {cmd}")
    if "service" not in req:
        return error("campo mancante: service")
    service = req["service"]
    if not isinstance(service, str) or service not in SERVICES:
        return error(f"servizio inesistente: {service}")
    host, port = SERVICES[service]
    return {"status": "ok", "service": service, "host": host, "port": port}


def handle_time(req):
    cmd = req.get("cmd")
    if cmd != "time":
        return error(f"comando sconosciuto: {cmd}")
    return {"status": "ok", "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}


def handle_echo(req):
    cmd = req.get("cmd")
    if cmd != "echo":
        return error(f"comando sconosciuto: {cmd}")
    if "text" not in req:
        return error("campo mancante: text")
    return {"status": "ok", "text": req["text"]}




async def ns_client(reader, writer):
    await serve_client(reader, writer, handle_nameserver, "name server")


async def time_client(reader, writer):
    await serve_client(reader, writer, handle_time, "time server")


async def echo_client(reader, writer):
    await serve_client(reader, writer, handle_echo, "echo server")


async def main():
    ns = await asyncio.start_server(ns_client, HOST, NS_PORT)
    ts = await asyncio.start_server(time_client, HOST, TIME_PORT)
    es = await asyncio.start_server(echo_client, HOST, ECHO_PORT)
    print(f"Name server su {HOST}:{NS_PORT}")
    print(f"Time server su {HOST}:{TIME_PORT}")
    print(f"Echo server su {HOST}:{ECHO_PORT}")

    # I tre server girano come task concorrenti
    tasks = [asyncio.create_task(s.serve_forever()) for s in (ns, ts, es)]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for s in (ns, ts, es):
            s.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    print("\nServer terminati.")