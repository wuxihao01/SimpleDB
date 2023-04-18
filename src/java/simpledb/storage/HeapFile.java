package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;

    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int index = BufferPool.getPageSize()*pid.getPageNumber();
//        System.out.println("readPage index:" + index); // debug
        byte[] data = HeapPage.createEmptyPageData();
        try {
            FileInputStream in = new FileInputStream(this.file);
            long r1 = in.skip(index);
            assert r1 == index;
            int r2 = in.read(data);
            assert r2 == BufferPool.getPageSize();
            in.close();
            HeapPageId hpid = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            return new HeapPage(hpid, data);
        } catch (FileNotFoundException e) {
            System.out.println("File not found!");
        } catch (IOException e) {
            System.out.println("IOException:" + e.toString());
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pid = page.getId();
        int pgNo = pid.getPageNumber();
        byte[] pgData = page.getPageData();
        RandomAccessFile rf = new RandomAccessFile(this.file, "rws");
        rf.skipBytes(pgNo * BufferPool.getPageSize());
        rf.write(pgData);
        rf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        long totalLen = this.file.length();
        return (int) totalLen / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> al = new ArrayList<>();
        for(int i = 0; i < numPages(); i++) {
            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPage pg = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if(pg != null && pg.getNumEmptySlots() > 0) {
                pg.insertTuple(t);
                al.add(pg);
                break;
            }
        }
        if(al.size() == 0) { // there is no empty slot on existing pages
            HeapPageId pid = new HeapPageId(getId(), numPages()); // create a new page
            HeapPage pg = new HeapPage(pid, HeapPage.createEmptyPageData());
            writePage(pg); // push the specified page to file (on disk)
            pg = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE); // also need to use through BufferPool
            if(pg == null) throw new DbException("Cannot get the target HeapPage!");
            pg.insertTuple(t);
            al.add(pg);
        }
        return al;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> al = new ArrayList<>();
        PageId pid = t.getRecordId().getPageId();
        if(pid.getTableId() != getId()) throw new DbException("Not a member of this file!");
        HeapPage pg = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        if(pg == null) throw new DbException("Cannot get the target HeapPage!");
        if(pg.isSlotUsed(t.getRecordId().getTupleNumber())) {
            pg.deleteTuple(t);
            al.add(pg);
        }
        return al;
    }

    // iterate through the tuples of each HeapPage in the HeapFile
    private static class HeapFileIterator implements DbFileIterator {

        private final TransactionId tid;

        private final int tableId;

        private final int pageNum;

        private int pgCursor;

        Iterator<Tuple> tupleIt;

        public HeapFileIterator(HeapFile file, TransactionId tid) {
            this.tid = tid;
            this.tableId = file.getId();
            this.pageNum = file.numPages();
            this.tupleIt = null;
            this.pgCursor = -1;
        }

        /**
         * Opens the iterator
         *
         * @throws DbException when there are problems opening/accessing the database.
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.pgCursor = 0;
            this.tupleIt = getTupleIt(0);
        }

        /**
         * @return true if there are more tuples available, false if no more tuples or iterator isn't open.
         */
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(this.pgCursor < 0) return false;
            if(this.tupleIt != null && this.tupleIt.hasNext()) return true;
            else {
                this.pgCursor++;
                while(this.pgCursor < this.pageNum) {
                    this.tupleIt = getTupleIt(this.pgCursor);
                    if(this.tupleIt != null && this.tupleIt.hasNext()) return true;
                    this.pgCursor++;
                }
            }
            return false;
        }

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(hasNext()) return this.tupleIt.next();
            else throw new NoSuchElementException("No More Elements!");
        }

        /**
         * Resets the iterator to the start.
         *
         * @throws DbException When rewind is unsupported.
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        /**
         * Closes the iterator.
         */
        @Override
        public void close() {
            this.pgCursor = -1;
            this.tupleIt = null;
        }

        // get the HeapPage's tuple iterator
        private Iterator<Tuple> getTupleIt(int cursor) throws TransactionAbortedException, DbException {
            HeapPageId hpid = new HeapPageId(this.tableId, cursor);
            HeapPage pg = (HeapPage) Database.getBufferPool().getPage(this.tid, hpid, Permissions.READ_ONLY); // get page through BufferPool
            return pg.iterator();
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

}

