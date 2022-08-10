import asyncio
from collections import defaultdict
from typing import Optional, Any, Dict, Set, List

WORKERS_COUNT = 10
MAX_PARALLEL_AGG_REQUESTS_COUNT = 5

ERROR_TIMEOUT_MSG = (
    "Слишком долгое ожидание, вероятно программа повисла в бесконечном ожидании"
)


class PipelineContext:
    def __init__(self, user_id: int, data: Optional[Any] = None):
        self._user_id = user_id
        self.data = data

    @property
    def user_id(self):
        return self._user_id


CURRENT_AGG_REQUESTS_COUNT = 0
BOOKED_CARS: Dict[int, Set[str]] = defaultdict(set)


async def get_offers(source: str) -> List[dict]:
    await asyncio.sleep(1)
    return [
        {"url": f"http://{source}/car?id=1", "price": 1_000, "brand": "LADA"},
        {"url": f"http://{source}/car?id=2", "price": 5_000, "brand": "MITSUBISHI"},
        {"url": f"http://{source}/car?id=3", "price": 3_000, "brand": "KIA"},
        {"url": f"http://{source}/car?id=4", "price": 2_000, "brand": "DAEWOO"},
        {"url": f"http://{source}/car?id=5", "price": 10_000, "brand": "PORSCHE"},
    ]


async def get_offers_from_sourses(sources: List[str]) -> List[dict]:
    global CURRENT_AGG_REQUESTS_COUNT
    if CURRENT_AGG_REQUESTS_COUNT >= MAX_PARALLEL_AGG_REQUESTS_COUNT:
        await asyncio.sleep(10.0)

    CURRENT_AGG_REQUESTS_COUNT += 1
    offers = await asyncio.gather(*[get_offers(source) for source in sources])
    CURRENT_AGG_REQUESTS_COUNT -= 1

    out = list()
    for offer in offers:
        out.extend(offer)
    return out


async def worker_combine_service_offers(inbound: asyncio.Queue,
                                        outbound: asyncio.Queue, sem):
    while True:
        ctx: PipelineContext = await inbound.get()
        async with sem:
            ctx.data = await get_offers_from_sourses(ctx.data)
        await outbound.put(ctx)


async def chain_combine_service_offers(inbound, outbound, **kw):
    sem = asyncio.Semaphore(MAX_PARALLEL_AGG_REQUESTS_COUNT)
    await asyncio.gather(
        *[asyncio.create_task(
            worker_combine_service_offers(inbound, outbound, sem))
            for _ in range(WORKERS_COUNT)]
    )


async def chain_filter_offers(
        inbound: asyncio.Queue,
        outbound: asyncio.Queue,
        brand: Optional[str] = None,
        price: Optional[int] = None,
        **kw,
):
    while True:
        ctx = await inbound.get()
        ctx.data = [
            car for car in ctx.data
            if (price is None or car['price'] <= price) and
               (brand is None or car['brand'] == brand)
        ]
        await outbound.put(ctx)


async def cancel_book_request(user_id: int, offer: dict):
    await asyncio.sleep(1)
    BOOKED_CARS[user_id].remove(offer.get("url"))


async def book_request(user_id: int, offer: dict, event: asyncio.Event) -> dict:
    try:
        BOOKED_CARS[user_id].add(offer.get("url"))
        await asyncio.sleep(1)
        if event.is_set():
            event.clear()
        else:
            await event.wait()
        return offer
    except asyncio.CancelledError:
        await cancel_book_request(user_id, offer)


async def worker_chain_book_car(inbound, outbound):
    while True:
        ctx = await inbound.get()
        event = asyncio.Event()
        event.set()
        done, pending = await asyncio.wait(
            [book_request(ctx.user_id, offer, event)
             for offer in ctx.data],
            return_when=asyncio.FIRST_COMPLETED
        )
        for do in done:
            ctx.data = do.result()
        for p in pending:
            p.cancel()
        await asyncio.gather(*pending)
        await outbound.put(ctx)


async def chain_book_car(inbound, outbound, **kw):
    await asyncio.gather(
        *[asyncio.create_task(worker_chain_book_car(inbound, outbound))
          for _ in range(WORKERS_COUNT)])


def run_pipeline(inbound):
    offers_outbound = asyncio.Queue()
    filtered_outbound = asyncio.Queue()
    result_outbound = asyncio.Queue()
    asyncio.create_task(chain_combine_service_offers(inbound, offers_outbound))
    asyncio.create_task(chain_filter_offers(offers_outbound, filtered_outbound))
    asyncio.create_task(chain_book_car(filtered_outbound, result_outbound))
    return result_outbound
