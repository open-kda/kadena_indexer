import sys
import os
import fcntl
import errno

# Add the kadena_indexer directory to Python path
kadena_indexer_path = os.path.join(os.path.dirname(__file__), 'kadena_indexer')
sys.path.insert(0, kadena_indexer_path)

from kadena_indexer.scriptsMaster import run_scripts

LOCK_FILE = '/tmp/run_scripts.lock'

def main():
    with open(LOCK_FILE, 'w') as lockfile:
        try:
            # Try to acquire an exclusive lock without blocking
            fcntl.flock(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError as e:
            if e.errno in (errno.EACCES, errno.EAGAIN):
                print("Script already running. Exiting.")
                return
            raise
        run_scripts()

if __name__ == "__main__":
    main()
