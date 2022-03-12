# 重置版本的Asyncio PSMB Client


## 使用高级抽象的Client

```python
from psmb_client.stream import Client
import asyncio

host = '127.0.0.1'
port = 13880
topic = '123123123'
client_id = 1

async def handler(msg: bytes):
    print(str(msg, encoding='UTF-8'))

client = Client(host, port, topic, client_id, handler)

async def main():
    try:
        await client.establish()
    except IOError:
        return
    for _ in range(10):
        try:
            await client.send_msg(b'asdasd')
            await asyncio.sleep(5)
        except IOError:
            break
    try:
        await client.close()
    except BrokenPipeError:
        return
    
asyncio.run(main())
```

## 流式、分离的Client

```python
from psmb_client.stream import *
import asyncio

host = '127.0.0.1'
port = 13880
topic = '123123123'
subsciber_id = 1

async def start_pub() -> Publisher:
    pub = Publisher(host, port, topic)
    await pub.open_connection()
    return pub

async def start_sub() -> Subscriber:
    
    async def process(data: bytes):
        print(str(data, 'UTF-8'))      
    sub = Subscriber(host, port, topic, subsciber_id, process)
    await sub.open_connection()
    return sub


async def main():
    pub, sub = await asyncio.gather(start_pub(), start_sub())
    for i in range(10):
        try:
            await pub.send_msg(b'asdasd')
            await asyncio.sleep(5)
        except IOError:
            break
    try:
        await pub.close()
        await sub.close()
    except BrokenPipeError:
        pass
    
asyncio.run(main())
```