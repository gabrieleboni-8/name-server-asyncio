import asyncio
import json

NS_HOST = "127.0.0.1"
NS_PORT = 6000


async def request(reader, writer, obj):
    """Invia una richiesta JSON e restituisce la risposta decodificata."""
    writer.write((json.dumps(obj) + "\n").encode())
    await writer.drain()
    line = await reader.readline()
    if not line:
        raise ConnectionError("connessione chiusa dal server")
    return json.loads(line.decode())


async def lookup(service):
    """Chiede al name server dove si trova il servizio."""
    reader, writer = await asyncio.open_connection(NS_HOST, NS_PORT)
    try:
        resp = await request(reader, writer, {"cmd": "lookup", "service": service})
    finally:
        writer.close()
        await writer.wait_closed()
    if resp.get("status") != "ok":
        raise RuntimeError(f"lookup di {service} fallito: {resp.get('message')}")
    return resp["host"], resp["port"]


async def call_service(service, req):
    """Lookup + connessione diretta al servizio + una richiesta."""
    host, port = await lookup(service)
    print(f"[client] {service} si trova su {host}:{port}")
    reader, writer = await asyncio.open_connection(host, port)
    try:
        return await request(reader, writer, req)
    finally:
        writer.close()
        await writer.wait_closed()


async def main():
    resp = await call_service("time", {"cmd": "time"})
    if resp["status"] == "ok":
        print("Ora ricevuta:", resp["time"])
    else:
        print("Errore time server:", resp["message"])

    resp = await call_service("echo", {"cmd": "echo", "text": "ciao"})
    if resp["status"] == "ok":
        print("Echo ricevuto:", resp["text"])
    else:
        print("Errore echo server:", resp["message"])

    # Prova di un servizio inesistente: l'errore viene gestito
    try:
        await lookup("xyz")
    except RuntimeError as e:
        print("Atteso:", e)


if __name__ == "__main__":
    asyncio.run(main())
