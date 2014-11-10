package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.LinkedList;
import java.util.Hashtable;

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
    
    private HashMap<PageId,Page> pages;
    private int numPages;	//maximum number of pages
    private HashMap<PageId,Integer> accessAr;
    private int accessNum;	//see evictPage() for description
    
    /*
     * ArrayList consists of: TrasactionId, boolean(granted/waiting), Permissions(mode)
     * If pid not in lockTable, there is not lock on the page 
     */
    private ConcurrentHashMap<PageId,LockManager> lockManagers;
        
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        pages = new HashMap<PageId,Page>();
        accessAr = new HashMap<PageId, Integer>();
        accessNum = 1;
        this.numPages = numPages;    
        lockManagers = new ConcurrentHashMap<PageId,LockManager>();
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
    	
		Page pageToReturn;			
		
		//page found in the buffer
		if(pages.containsKey(pid)){
			accessAr.remove(pid);
			accessAr.put(pid, accessNum++);
			pageToReturn = pages.get(pid);
			//lock manager must already exist
			lockManagers.get(pid).acquireLock(tid, perm);	
			
		//page not in the buffer
		}else{      
			int t = pid.getTableId();
		    DbFile f = Database.getCatalog().getDatabaseFile(t);
		    pageToReturn = f.readPage(pid);
		    
		    //if there is no space in the buffer pool, evict page
		    if(numPages==pages.size()){
		     	evictPage();
		    }
		    pages.put(pid, pageToReturn);
	        accessAr.put(pid, accessNum++);
	        
	        //create lock manager for the page
	        LockManager newLockManager = new LockManager();
	        newLockManager.acquireLock(tid, perm);
	        lockManagers.put(pid, newLockManager);
		}	
		
		return pageToReturn;		
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
    	lockManagers.get(pid).releaseLock();
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
    	return lockManagers.get(p).inUse;
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
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	DbFile f = Database.getCatalog().getDatabaseFile(tableId);
    	ArrayList<Page> dirtyPages = f.insertTuple(tid, t); //pages that were dirtied
    	Iterator<Page> dirtyItr = dirtyPages.iterator();		
    	
    	while (dirtyItr.hasNext()){
    		HeapPage dirtyPage = (HeapPage)dirtyItr.next();
    		dirtyPage.markDirty(true, tid);
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
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	int tableId = t.getRecordId().getPageId().getTableId();
    	DbFile file = Database.getCatalog().getDatabaseFile(tableId);
    	ArrayList<Page> dirtyPages = file.deleteTuple(tid, t); //pages that were dirtied
    	Iterator<Page> dirtyItr = dirtyPages.iterator();
    	
    	while (dirtyItr.hasNext()){
    		HeapPage dirtyPage = (HeapPage)dirtyItr.next();
    		dirtyPage.markDirty(true, tid);
    	}
    	dirtyItr.remove();
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
    	Set<PageId> ps = pages.keySet();
    	Iterator<PageId> it = ps.iterator();
    	while(it.hasNext()){
    		PageId pi = it.next();
    		if(pages.containsKey(pi)){
    			flushPage(pi);
    		}
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
    	if(pages.containsKey(pid)){
    		Page p = pages.get(pid);
		    //find dirty page
		    if(p.isDirty()!=null){
		    	try{
		    		DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());		    		
					f.writePage(p); //write page to disk
					return;
		    	}catch(IOException e){
		    		throw new IOException("cannot find page");
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
    	/* 
    	 * LRU page eviction policy
    	 * 
    	 * Array accessAr keeps track of the order of page access. Every time
    	 * a page is accessed through getPage, insertTuple, or deleteTuple,
    	 * the corresponding integer in accessAr will be updated to the number
    	 * of pages that has been accessed so far, including the page itself.
    	 * Pages that have never been accessed will have zero in acessAr.
    	 * Higher the value in accessAr, more recently the corresponding
    	 * page was accessed.
    	 * 
    	 * Code below gets the page with the smallest value in accessAr
    	 * and evicts the page. 
    	 */
    	
    	//choose page
    	Set<PageId> pa = pages.keySet();    	
    	PageId pp = (PageId)pa.toArray()[0];	//arbitrary pid 
    	int min = accessAr.get(pp);				//arbitrary number 
    	
    	Iterator<PageId> it = pa.iterator();
    	while(it.hasNext()){
    		PageId i = it.next();
    		int n = accessAr.get(i);
    		if(min>n){
    			min = n;
    			pp = i;
    		}
    	}
    	
    	//evict the chosen page
    	try{
	    	flushPage(pp);
	    	pages.remove(pp);
	    	accessAr.remove(pp);
    	}catch(IOException e){
    		throw new DbException("cannot evit page");
    	}
    }
    
    static class LockManager {
    	private boolean inUse = false;
    	
        public synchronized void acquireLock(TransactionId tid, Permissions perm){    		
    		boolean waiting = true;
    		while(waiting){          		
              	if(!inUse){ 
               		inUse = true;
               		waiting = false;
               	}else{
               		try{
    					wait();
    				}catch(InterruptedException e) {}
               	}	            
    		}			
        }

        public synchronized void releaseLock() {
            inUse = false;
            notifyAll(); //once done, notify threads that are waiting
        }
    }
}
