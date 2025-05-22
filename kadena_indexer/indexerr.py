import asyncio
import logging
from dataclasses import asdict

import yaml
from easydict import EasyDict
from pymongo import MongoClient
from .coordinator import Coordinator
from .chainweb import ChainWeb
from .NG_scripts.collections import process_collections

logger = logging.getLogger(__name__)

class Indexer:
    """ Main indexer class """

    def __init__(self, config_file):
        self._tips = {}
        self.config = self._load_config(config_file)
        self.mongo_client = MongoClient(self.config.mongo_uri)
        logger.info("Connected to MongoDB v{!s}".format(self.mongo_client.server_info()["version"]))
        self.db = self.mongo_client[self.config.db]
        self.coordinator = self._load_coordinator()
        self._check_indexes()
        self._prune_db()


    def _load_config(self, config_file):
        logger.info("Loading config {}".format(config_file))
        with open(config_file, "rb") as fd:
            return EasyDict(yaml.safe_load(fd))

    def _load_coordinator(self):
        logger.info("Loading coordinator")
        c = Coordinator(self.db.coordinator)
        for ev in self.config.events:
            for chain in ev.chains:
                c.register_event(chain, ev.name, ev.height)
        return c

    def _prune_db(self):
        logger.info("Pruning Database")
        for (name, chain, lower, upper) in self.coordinator.get_wanted():
            res = self.db[name].delete_many({"chain":chain, "$or":[{"height":{"$lt":lower}}, {"height":{"$gt":upper}}]})
            if res.deleted_count:
                logger.info("Pruned {:d} events for {:s}/{: <2}".format(res.deleted_count, name, chain))

    def _check_indexes(self):
        """ This checks all the reqired indexes: coordinator collection + events collection """
        logger.info("Updating indexes")
        if "name_chain" not in self.db.coordinator.index_information():
            logger.info("Create coordinator index")
            self.db.coordinator.create_index(["name", "chain"], name="name_chain")

        for ev in self.config.events:
            coll = self.db[ev.name]
            current_idx = coll.index_information()
            for idx_field in ["regKey", "height", "block", "ts"]:
                idx_name = "st_"+idx_field
                if idx_name not in current_idx:
                    logger.warning("{} => Index {} missing".format(ev.name, idx_name))
                    coll.create_index(idx_field, name=idx_name)

            # Special index required for pruning
            if "st_prune" not in current_idx:
                logger.warning("{} => Index {} missing".format(ev.name, "st_prune"))
                coll.create_index({"chain":1, "height":1}, name="st_prune")


    def _index_block(self, blk, log_height=0):
        with self.mongo_client.start_session() as session:
            with session.start_transaction():
                for e in blk.events():
                    if self.coordinator.should_index_event(e.chain, e.name, e.height):
                        self.db[e.name].insert_one(asdict(e), session=session)
                self.coordinator.validate_block(blk.chain, blk.height, session=session)

        if log_height and blk.height % log_height == 0:
            logger.info("Chain {:<2}: Indexed block {:d}".format(blk.chain, blk.height))


    async def _fill_missing_blocks(self, cw, ref_blk):
        for it in reversed(self.coordinator.get_missing(ref_blk.chain, ref_blk.height-1)):
            logger.info("Chain {:<2}: Fill hole {:d} -> {:d}".format(ref_blk.chain, it.lower, it.upper))
            async for b in cw.get_blocks(ref_blk.chain, ref_blk.block_hash, it.lower, it.upper):
                self._index_block(b,1000)
            logger.info("Chain {:<2}: Fill hole completed {:d} -> {:d}".format(ref_blk.chain, it.lower, it.upper))


    async def _fill_missing_blocks_task(self, cw, chain):
        while True:
            try:
                await self._fill_missing_blocks(cw, self._tips[chain])
                await asyncio.sleep(120.0)
            except asyncio.CancelledError:
                logger.info("Chain {:<2}: => Ended".format(chain))
                return
            except Exception as e: # pylint: disable=broad-except
                logger.error("Chain {:<2}: Error when filling blocks: {!s}".format(chain, e))
                await asyncio.sleep(120.0)



    async def run(self):
        """ Async function to start the indexer """
        task_started = {}
        async with ChainWeb(self.config.node) as cw:
            logger.info("Start listening CW node")
            try:
                async for b in cw.get_new_block():
                    self._index_block(b, 200)
                    self._tips[b.chain] = b
                    if b.chain not in task_started:
                        task_started[b.chain] = asyncio.create_task(self._fill_missing_blocks_task(cw, b.chain))
            except asyncio.CancelledError:
                logger.info("Cancelled")
                for tsk in task_started.values():
                    tsk.cancel()
                    #await tsk
