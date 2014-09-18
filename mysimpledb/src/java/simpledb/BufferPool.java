package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p/>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    public static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;
    
    public PageId[] pidAr; 	//array of PageIds
    public Page[] pageAr; 	//array of Pages
    public int[] accessAr;
    public int numPages;	//actual number of pages (not the same as pidAr.length)
        
    //public HashMap<PageId,Page> pages;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        pidAr = new PageId[numPages];
        pageAr = new Page[numPages];
        accessAr = new int[numPages];
        this.numPages = 0;
        
        //pages = new HashMap<PageId,Page>(numPages);
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p/>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {    	
        for(int i=0; i<pidAr.length;i++){
        	if(pidAr[i]!=null){
        		if(pidAr[i].equals(pid)){
        			accessAr[i]++;
        			return pageAr[i];
        		}
       		}
        }
        
        //if the page cannot be found in the buffer pool, find it from the DbFile
        Catalog c = Database.getCatalog();
        int t = pid.getTableId();
        DbFile f = c.getDatabaseFile(t);
        Page newPage = f.readPage(pid);
        
        //if there is no space in the buffer pool, evict page
        if(numPages==pidAr.length){
        	evictPage();
        }
        
        //put the page in buffer pool and return the page
        if(numPages<pidAr.length){
        	for(int i=0; i<pidAr.length; i++){
        		if(pidAr[i]==null){
        			pidAr[i]=pid;
        			pageAr[i]=newPage;
        			numPages++;
        			return newPage;
        		}
        	}
        }
        throw new DbException("page could not be found");
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed until lab5).                                  // cosc460
     * May block if the lock(s) cannot be acquired.
     * <p/>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    //wasn't tested
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	DbFile f = Database.getCatalog().getDatabaseFile(tableId);
    	ArrayList<Page> dirtiedFiles = f.insertTuple(tid, t);
    	Iterator<Page> dirtyItr = dirtiedFiles.iterator();
    	while (dirtyItr.hasNext()){
    		HeapPage h = (HeapPage)dirtyItr.next();
    		PageId pid = h.getId();
    		h.markDirty(true, tid);
    		for(int i=0; i<pidAr.length;i++){
    			if(pid.equals(pidAr[i])){
    				accessAr[i]++;
    				pageAr[i]=h;
    			}
    		}
    	}
    	dirtyItr.remove();
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p/>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    //wasn't tested
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	PageId pid = t.getRecordId().pid;
    	for(int i=0; i<pidAr.length;i++){
    		if(pid.equals(pidAr[i])){
    			HeapPage h = (HeapPage)pageAr[i];
    			h.deleteTuple(t);
    			h.markDirty(true, tid);
    			pageAr[i]=h;
    			accessAr[i]++;
    		}
    	}
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for(int i=0;i<numPages;i++){
        	flushPage(pidAr[i]);        	
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab6                                                                            // cosc460
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
    	DbFile f;
    	for(int i=0; i<numPages; i++){
    		if(pidAr[i].equals(pid)){
    			Page p = pageAr[i];
    			if(p.isDirty()!=null){
    				try{
	    				f = Database.getCatalog().getDatabaseFile(pid.getTableId());
	    				f.writePage(p);
    				}catch(IOException e){
    					throw new IOException("cannot find page");
    				}
    			}
    		}
    	}
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
    	int min = accessAr[0];
    	int index = 0;
    	for(int i=1;i<pidAr.length;i++){
    		if(min>accessAr[i]){
    			min = accessAr[i];
    			index = i;    					
    		}    	
    	}
    	try{
	    	flushPage(pidAr[index]);
	    	pidAr[index]=null;
	    	pageAr[index]=null;
	    	accessAr[index]=0;
	    	numPages--;
    	}catch(IOException e){
    		e.printStackTrace();
    	}
    }

}
