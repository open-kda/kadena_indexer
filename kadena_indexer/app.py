import logging
import asyncio
import argparse

from .indexer import Indexer

def main():
    """ Main method of the indexer, init the loger, the indexer and run it """

    parser = argparse.ArgumentParser(prog='kadena_indexer', description='Index a Kadena Blockchain')
    parser.add_argument('config_file', help="YAML Config file")
    parser.add_argument('-d', '--debug', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(encoding='utf-8', format='%(asctime)s:%(levelname)s:%(name)s => %(message)s', level=logging.DEBUG if args.debug else logging.INFO)
    idx = Indexer(args.config_file)
    asyncio.run(idx.run())

if __name__ == "__main__":
    main()
