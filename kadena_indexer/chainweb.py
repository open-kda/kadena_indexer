import asyncio
from dataclasses import dataclass
import json
from functools import partial
from datetime import datetime
import logging
import orjson

import aiohttp
from cachetools import FIFOCache
from bson.decimal128 import Decimal128

from .kadena_common import b64_decode

logger = logging.getLogger(__name__)

BLOCKS_PER_BATCH = 300

def pact_hook(x):
    """ Pact hook for the JSON deserializer """
    if "decimal" in x:
        try:
            return Decimal128(x["decimal"])
        except Exception: # pylint: disable=broad-except
            # We are probably here facing to 
            return x
    if "int" in x:
        v = int(x["int"])
        return v if v.bit_length() <= 64 else str(v)
    return x

# pylint: disable=missing-function-docstring, multiple-statements
def module_fqn(x): return "{0[namespace]:s}.{0[name]:s}".format(x)  if x["namespace"] else x["name"]

def event_fqn(x): return "{}.{}".format(module_fqn(x["module"]), x["name"])

json_load = partial(json.loads, parse_float=Decimal128, object_hook=pact_hook)

def decode_cb(x): return json_load(b64_decode(x))

def decode_tx(x): return json_load(b64_decode(x[1]))
# pylint: enable=missing-function-docstring, multiple-statements


@dataclass
class Event:
    """ Dataclass that represents a Chainweb event """
    name:str
    params:list
    reqKey:str
    chain:str
    block:str
    rank:int
    height:int
    ts:datetime


class ChainWebBlock:
    """ Reprensent a Kadena / Chainweb block """
    def __init__(self, data):
        self.block_hash = data["header"]["hash"]
        self.height =  data["header"]["height"]
        self.parent =  data["header"]["parent"]
        self.chain = str(data["header"]["chainId"])
        self.ts = datetime.fromtimestamp(data["header"]["creationTime"]/1e6)
        self.payload = data["payloadWithOutputs"]

    def transactions_output(self):
        """ Return the transactions output of the block """
        yield decode_cb(self.payload["coinbase"])
        yield from map(decode_tx, self.payload["transactions"])

    def events(self):
        """ Return all the events emitted by the block """
        for rank, trx in enumerate(self.transactions_output()):
            if "events" in trx:
                yield from map(lambda x: Event(event_fqn(x),x["params"], trx["reqKey"] , self.chain, self.block_hash, rank, self.height, self.ts), trx["events"]) #pylint: disable=cell-var-from-loop


class ChainWeb:
    """ Mainclass that handles all Chainweb communications stuffs """
    def __init__(self, url):
        self._chainweb_node = url
        self._network = None
        self.session = None
        self.network = None
        self.cache = FIFOCache(256)

    async def __aenter__(self):
        tmout = aiohttp.ClientTimeout(sock_read=30.0, connect=30.0)
        self.session = await aiohttp.ClientSession(timeout=tmout, read_bufsize=1024*1024).__aenter__()

        logger.info("Retrieving Chainweb info")
        async with self.session.get(self.info_url) as resp:
            info = await resp.json()
            logger.info("Node version: {:s}".format(info["nodePackageVersion"]))
            logger.info("Network: {:s}".format(info["nodeVersion"]))
            self._network = info["nodeVersion"]

        return self

    async def __aexit__(self, *args):
        await self.session.__aexit__(*args)

    @property
    def info_url(self):
        """ Info URL of the node"""
        return self._chainweb_node + "/info"

    @property
    def api_url(self):
        """ API Base URL of the node"""
        return "{:s}/chainweb/0.0/{:s}".format(self._chainweb_node, self._network)

    async def get_blocks(self, chain, parent, min_height, max_height):
        """ Return an iterator through a range of blocks from a chain, with the help of a parent block """
        body = {"lower":[], "upper":[parent]}

        for mah in range(max_height, min_height-1, -BLOCKS_PER_BATCH):
            _next = "INIT"
            while _next:
                params = {"limit":150, "minheight":max(mah - BLOCKS_PER_BATCH, min_height), "maxheight":mah}
                if _next != "INIT":
                    params["next"] = _next

                async with self.session.post("{:s}/chain/{:s}/block/branch".format(self.api_url, chain), params=params, json=body) as resp:
                    data = orjson.loads(await resp.read())
                    # data = await resp.json()
                    for blk in map(ChainWebBlock, data["items"]):
                        yield blk
                    _next = data["next"]

    @staticmethod
    async def _parse_block_stream(content):
        while True:
            data = await content.readline()
            record = data.strip().split(b":", 1)
            if len(record) == 2  and record[0] == b"data":
                yield ChainWebBlock(json.loads(record[1]))

    async def get_new_block(self):
        """ Return an iterator of new (streamed blocks) """
        while True:
            first = True
            try:
                async with self.session.post(self.api_url +"/block/updates") as resp:
                    async for blk in self._parse_block_stream(resp.content):

                        if first:
                            logger.info("Block stream OK")
                            first = False

                        if blk.parent in self.cache:
                            yield self.cache[blk.parent]
                        self.cache[blk.block_hash] = blk

            except Exception:  # pylint: disable=broad-except
                logger.exception("Error when reading block stream")
                await asyncio.sleep(10.0)
                logger.info("Trying to reconnect")
