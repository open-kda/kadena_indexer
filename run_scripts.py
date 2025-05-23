import sys
import os
import fcntl
import errno
import time

# Add the kadena_indexer directory to Python path
kadena_indexer_path = os.path.join(os.path.dirname(__file__), 'kadena_indexer')
sys.path.insert(0, kadena_indexer_path)

from kadena_indexer.scriptsMaster import run_scripts

LOCK_FILE = '/tmp/run_scripts.lock'

def is_process_running(pid):
    """Check if a process with given PID is still running"""
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False

def acquire_lock():
    """Acquire lock with stale lock detection"""
    # Check if lock file exists and if the process is still running
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, 'r') as f:
                old_pid = int(f.read().strip())
            
            if is_process_running(old_pid):
                print("Another instance of the script is running. Exiting.")
                return None
            else:
                print("Removing stale lock file...")
                os.remove(LOCK_FILE)
        except (ValueError, FileNotFoundError):
            # Invalid or missing PID, remove the lock file
            try:
                os.remove(LOCK_FILE)
            except FileNotFoundError:
                pass
    
    # Create new lock file with current PID
    try:
        lockfile = open(LOCK_FILE, 'w')
        fcntl.flock(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lockfile.write(str(os.getpid()))
        lockfile.flush()
        return lockfile
    except IOError as e:
        if e.errno in (errno.EACCES, errno.EAGAIN):
            print("Another instance of the script is running. Exiting.")
            return None
        raise

def main():
    print("Starting script execution...")
    
    lockfile = acquire_lock()
    if lockfile is None:
        return
    
    try:
        run_scripts()
    finally:
        # Clean up lock file
        try:
            lockfile.close()
            os.remove(LOCK_FILE)
        except:
            pass

if __name__ == "__main__":
    main()