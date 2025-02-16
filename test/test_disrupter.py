import pytest
import asyncio
from disruptor import Disruptor, Consumer


class TestConsumer(Consumer):
    def __init__(self):
        self.consumed_elements = []

    async def consume(self, elements):
        self.consumed_elements.extend(elements)


@pytest.mark.asyncio
async def test_disruptor():
    disruptor = Disruptor(size=10)
    consumer = TestConsumer()
    disruptor.register_consumer(consumer)

    await disruptor.produce([1, 2, 3, 4, 5])
    await asyncio.sleep(1)  # give some time for consumers to process

    assert consumer.consumed_elements == [1, 2, 3, 4, 5]

    await disruptor.close()

@pytest.mark.asyncio
async def test_disruptor_full():
    disruptor = Disruptor(size=3)
    consumer = TestConsumer()
    disruptor.register_consumer(consumer)

    await disruptor.produce([1, 2, 3])
    await asyncio.sleep(1)  # give some time for consumers to process

    assert consumer.consumed_elements == [1, 2, 3]

    await disruptor.produce([4, 5, 6])
    await asyncio.sleep(1)  # give some time for consumers to process

    assert consumer.consumed_elements == [1, 2, 3, 4, 5, 6]

    await disruptor.close()
