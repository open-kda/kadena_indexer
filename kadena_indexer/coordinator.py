from itertools import starmap
import logging

from pymongo import ReplaceOne
import portion

logger = logging.getLogger(__name__)

class IntInterval(portion.AbstractDiscreteInterval):
    """ Helper class for managing integers intervals within portion """
    _step = 1

P = portion.create_api(IntInterval)

MIN_HEIGHT = 1138000 #No Pact events before that height
MAX_HEIGHT = 999999999

def norm_range(x):
    """ Normalize a range coming for the config """
    if not x:
        return (MIN_HEIGHT, MAX_HEIGHT)
    (xl, xh) = x
    return (max(MIN_HEIGHT, xl), min(MAX_HEIGHT, xh) if xh else MAX_HEIGHT)


ALL_CHAINS = [str(x) for x in range(0,20)]

class Coordinator:
    """ Class that manages all events indexing states """

    # This class works by managing two obejcts.
    #  - wanted: Events ranges that comes from the config
    #  - done: Events ranges already indexed.. These ranges are always narrower than wanted.
    #          This object is written in the MongoDB
    def __init__(self, mongo_collection):
        self.wanted = {c:{} for c in ALL_CHAINS}
        self.done = {c:{} for c in ALL_CHAINS}
        self.collection = mongo_collection

    def register_event(self, chain, name, height_range):
        """ Register an event to indexr for a given chain, and an height range (2-tuple) """
        logger.info("Using {:s}/{: <2} => {!s}".format(name, chain, norm_range(height_range)))
        self.wanted[chain][name] = P.closed(*norm_range(height_range))

        # Get Done from MongoDB
        data = self.collection.find_one({"chain":chain, "name":name})
        done = P.from_data(data["range"]) if data is not None else P.empty()

        # We intersect with Wanted
        done &= self.wanted[chain][name]
        self.done[chain][name] = done

        #And update MongoDB just in case
        self.collection.replace_one({"chain":chain, "name":name},  {"chain":chain, "name":name, "range":P.to_data(done)}, True)


    def should_index_event(self, chain, name, height):
        """ Return true is the given event has to be indexed """
        return name in self.wanted[chain] and height in self.wanted[chain][name] and not height in self.done[chain][name]

    def _validate_blocks(self, chain, height_range, session=None):
        updates = []

        for (name, done), wanted in zip(self.done[chain].items(), self.wanted[chain].values()):
            new_done = (done | height_range) & wanted
            if new_done != done:
                self.done[chain][name] = new_done
                updates.append(ReplaceOne({"chain":chain, "name":name},  {"chain":chain, "name":name, "range":P.to_data(new_done)}, upsert=True))

        if updates:
            self.collection.bulk_write(updates, ordered=False, session=session)

    def validate_blocks(self, chain, min_height, max_height, session=None):
        """ Notify the coordinator that a range of blocks has been indexed """
        self._validate_blocks(chain, P.closed(min_height, max_height), session=session)

    def validate_block(self, chain, height, session=None):
        """ Notify the coordinator that a given block has been indexed """
        self._validate_blocks(chain, P.singleton(height), session=session)

    def get_missing(self, chain, max_height):
        """ Return the missing ranges (to be indexed) for a given chain """
        result = P.empty()
        for wanted, done in zip(self.wanted[chain].values(), self.done[chain].values()):
            result |= wanted - done

        return result & P.closed(MIN_HEIGHT, max_height)

    def get_wanted(self):
        """ Returns a flattened view of the wanted events in a list of tuples (event, chain, renge_low, range_high)"""
        for chain, ev in self.wanted.items():
            yield from starmap(lambda k,v:(k, chain, v.lower, v.upper), ev.items()) #pylint: disable=cell-var-from-loop
