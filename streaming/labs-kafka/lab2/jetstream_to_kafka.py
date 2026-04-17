import asyncio, json, re, os
import orjson
import websockets
from aiokafka import AIOKafkaProducer

JETSTREAM_URL = os.getenv(
    "JETSTREAM_URL",
    "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "social.posts")

hashtag_re = re.compile(r"(?:^|[\s])#([A-Za-z0-9_]+)")
mention_re = re.compile(r"(?:^|[\s])@([A-Za-z0-9_.-]+)")

def extract_tags_mentions(text: str):
    if not text:
        return [], []
    tags = [m.group(1) for m in hashtag_re.finditer(text)]
    mnts = [m.group(1) for m in mention_re.finditer(text)]
    # normaliza menciones a formato "@handle"
    mnts = [f"@{m}" for m in mnts]
    return tags, mnts

def build_record(evt: dict):
    """
    Jetstream entrega JSON con campos como:
    {
      "kind":"commit",
      "time_us":"...",
      "commit":{
        "operation":"create",
        "collection":"app.bsky.feed.post",
        "rkey":"3lxy...",
        "record":{"$type":"app.bsky.feed.post","text":"...","createdAt":"...","langs":["es"]},
        "repo":"did:plc:...."   // DID del autor
      },
      "did":"did:plc:...."      // suele repetirse
    }
    """
    c = evt.get("commit") or {}
    rec = c.get("record") or {}
    if not rec:
        return None

    text = rec.get("text", "")
    tags, mnts = extract_tags_mentions(text)

    uri = None
    if c.get("collection") and c.get("rkey") and (evt.get("did") or c.get("repo")):
        uri = f"at://{evt.get('did', c.get('repo'))}/{c['collection']}/{c['rkey']}"

    out = {
        "id": c.get("rkey"),
        "did": evt.get("did") or c.get("repo"),
        "created_at": rec.get("createdAt"),
        "text": text,
        "langs": rec.get("langs") or [],
        "hashtags": tags,
        "mentions": mnts,
        "uri": uri,
        "raw": evt,  # útil para debug en clase
    }
    return out

async def run():
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: orjson.dumps(v)
    )
    await producer.start()
    try:
        while True:
            try:
                async with websockets.connect(JETSTREAM_URL, open_timeout=30) as ws:
                    print("Conectado a Jetstream:", JETSTREAM_URL)
                    async for message in ws:
                        try:
                            evt = json.loads(message)
                            # Jetstream ya viene filtrado a posts por 'wantedCollections'
                            if evt.get("kind") != "commit":
                                continue
                            if evt.get("commit", {}).get("operation") != "create":
                                continue
                            rec = build_record(evt)
                            if rec:
                                # clave por DID para particionado estable
                                key = (rec["did"] or "no-did").encode("utf-8")
                                await producer.send_and_wait(TOPIC, rec, key=key)
                                print("→ published:", rec["created_at"], rec["did"], rec["text"][:60])
                        except Exception as e:
                            print("Error procesando mensaje:", e)
            except Exception as e:
                print("WS desconectado / error:", e)
                await asyncio.sleep(3)
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(run())


