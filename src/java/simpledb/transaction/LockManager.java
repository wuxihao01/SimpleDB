package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockManager is used to manage locks for transactions.
 */
public class LockManager {
    private static class Lock {
        final private TransactionId tid;
        private int lockType;

        public Lock(TransactionId tid, int lockType) {
            this.tid = tid;
            this.lockType = lockType;
        }

        public TransactionId getTid() {
            return tid;
        }

        public int getLockType() {
            return lockType;
        }

        public void setLockType(int lockType) {
            this.lockType = lockType;
        }
    }
    private final Map<PageId, List<Lock>> lockMap;

    public LockManager() {
        lockMap = new ConcurrentHashMap<>();
    }

    public boolean tryAcquireLock(PageId pid, TransactionId tid, int type, int timeout) {
        long now = System.currentTimeMillis();
        while(true) {
            if(System.currentTimeMillis() - now > timeout) {
                return false;
            }
            if(acquireLock(pid, tid, type)) {
                return true;
            }
        }
    }

    /**
     * This function ensure atomicity between "Test" and "Set".
     * @param pid The page to acquire lock from.
     * @param tid The transaction trying to acquire a lock.
     * @param type The type of lock to require. 0: read_only 1: read_write
     * @return True if successfully acquire a lock.
     */
    public synchronized boolean acquireLock(PageId pid, TransactionId tid, int type) {
        if(lockMap.containsKey(pid)) {
            List<Lock> locks = lockMap.get(pid);
            if(type == 0) {
                for(Lock l: locks) {
                    if(l.getTid().equals(tid)) return true;
                    if(!l.getTid().equals(tid) && l.getLockType() == 1) return false;
                }
                locks.add(new Lock(tid, 0));
                lockMap.put(pid, locks);
                return true;
            }
            else { // type = 1
                for(Lock l: locks) {
                    if(!l.getTid().equals(tid)) return false;
                }
                locks.clear();
                locks.add(new Lock(tid, 1));
                lockMap.put(pid, locks);
                return true;
            }
        }
        else { // PageId is not in LockMap
            List<Lock> locks = new ArrayList<>();
            locks.add(new Lock(tid, type));
            lockMap.put(pid, locks);
            return true;
        }
    }

    /**
     * see unsafeReleasePage in BufferPool for details.
     */
    public synchronized void releaseLock(PageId pid, TransactionId tid) {
        if(lockMap.containsKey(pid)) {
            List<Lock> locks = lockMap.get(pid);
            for(int i = 0; i < locks.size(); i++) { // very strange here
                Lock l = locks.get(i);
                if(l.getTid().equals(tid)) locks.remove(l);
            }
            if(locks.size() == 0) lockMap.remove(pid);
            else lockMap.put(pid, locks);
        }
    }

    public synchronized void releaseAllLocks(TransactionId tid) {
        Object[] pids = lockMap.keySet().toArray(); // must make a copy clone here
        for(Object pid: pids) {
            releaseLock((PageId) pid, tid);
        }
    }

    public synchronized void releasePage(PageId pid) {
        lockMap.remove(pid);
    }

    public synchronized boolean holdsLock(PageId pid, TransactionId tid) {
        if(!lockMap.containsKey(pid)) return false;
        List<Lock> locks = lockMap.get(pid);
        for(Lock l: locks) {
            if(l.getTid().equals(tid)) return true;
        }
        return false;
    }
}
