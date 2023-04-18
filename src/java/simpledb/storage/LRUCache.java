package simpledb.storage;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LRUCache {

    public static class ListNode<K, V> {
        private K key; // PageId
        private V val; // Page
        ListNode<K, V> prev;
        ListNode<K, V> next;

        public ListNode(K key, V val) {
            this.key = key;
            this.val = val;
        }

        public ListNode() { }
    }

    private final Map<PageId, ListNode<PageId, Page>> cache;
    private final ListNode<PageId, Page> head;
    private final ListNode<PageId, Page> tail;

    public LRUCache() {
        this.cache = new ConcurrentHashMap<>();
        this.head = new ListNode<>();
        this.tail = new ListNode<>();
        this.head.prev = null;
        this.head.next = tail;
        this.tail.prev = head;
        this.tail.next = null;
    }

    public void put(PageId pid, Page pg) {
        ListNode<PageId, Page> t = cache.get(pid);
        if(t != null) { // move t to the head
            t.val = pg; // val may have been changed
            t.next.prev = t.prev;
            t.prev.next = t.next;
            t.prev = head;
            t.next = head.next;
            head.next.prev = t;
            head.next = t;
            cache.put(pid, t);
        }
        else {
//            if(cache.size() >= capacity) eviction();
            ListNode<PageId, Page> n = new ListNode<>(pid, pg);
            n.prev = head;
            n.next = head.next;
            head.next.prev = n;
            head.next = n;
            cache.put(pid, n);
        }
    }

    public Page get(PageId pid) {
        ListNode<PageId, Page> t = cache.get(pid);
        if(t != null) {
            t.next.prev = t.prev;
            t.prev.next = t.next;
            t.prev = head;
            t.next = head.next;
            head.next.prev = t;
            head.next = t;
            cache.put(pid, t);
            return t.val;
        }
        else return null;
    }

    public void remove(PageId pid) {
        ListNode<PageId, Page> t = cache.get(pid);
        if(t != null) {
            t.next.prev = t.prev;
            t.prev.next = t.next;
            cache.remove(pid);
        }
    }

    public PageId eviction() {
        ListNode<PageId, Page> t = tail.prev;
//        get(t.key); // this page may be dirty, move to the head
        return t.key;
    }

    public Set<PageId> keySet() {
        return cache.keySet();
    }

    public int getSize() {
        return cache.size();
    }
}
