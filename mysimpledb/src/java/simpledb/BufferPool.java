package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
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
    private int maxPages;						//maximum number of pages
    private HashMap<PageId,Integer> accessAr;	//see evictPage() for description
    private int accessNum;						//see evictPage() for description
    
    //Keep track of pages and their lock managers
    private ConcurrentHashMap<PageId,LockManager> lockManagers;
        
    //Keep track of which transaction accessed/modified which page
    private ConcurrentHashMap<TransactionId,Set<PageId>> tidMap;
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        pages = new HashMap<PageId,Page>();
        accessAr = new HashMap<PageId, Integer>();
        accessNum = 1;
        maxPages = numPages;    
        lockManagers = new ConcurrentHashMap<PageId,LockManager>();
        tidMap = new ConcurrentHashMap<TransactionId,Set<PageId>>();
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
    	
		Page pageToReturn = null;			
		
		//page found in the buffer
		if(pages.containsKey(pid)){
			synchronized(this){
				accessAr.remove(pid);
				accessAr.put(pid, accessNum++);
				pageToReturn = pages.get(pid);			
				//System.out.println("page was found in buffer: "+pageToReturn.getId());
				//lock manager must already exist
				lockManagers.get(pid).acquireLock(tid, perm);
			}
			
		//page not in the buffer
		}else{      
			int t = pid.getTableId();
		    DbFile f = Database.getCatalog().getDatabaseFile(t);
		    pageToReturn = f.readPage(pid);
		    //System.out.println("page was found in disk: "+pageToReturn.getId());
		    
		    //if there is no space in the buffer pool, evict page
		    if(maxPages==pages.size()){
		     	evictPage();
		    }
		    synchronized(this){
		    	pages.put(pid, pageToReturn);
		    	accessAr.put(pid, accessNum++);
		    }
	        
	        //create lock manager for the page
	        LockManager newLockManager = new LockManager();	 
	        synchronized(this){
	        	lockManagers.put(pid, newLockManager);
	        	newLockManager.acquireLock(tid, perm);
	        }	        
		}	
		synchronized(this){
			//add information to tidMap
			if(!tidMap.containsKey(tid)){
				Set<PageId> s = new HashSet<PageId>();
				s.add(pid);
				tidMap.put(tid, s);			
			}else{
				tidMap.get(tid).add(pid);
			}
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
    	lockManagers.get(pid).releaseLock(tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid,true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
    	return lockManagers.get(p).txns.get(tid);
    }        
    
    /*
     * Return the number of transactions currently holding lock on the specified page
     * Used in HeapFile.java to see if the lock was just acquired
     */
    public int numTransactions(PageId p){
    	return lockManagers.get(p).num;
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
    	
    	Set<PageId> dirtyPages = tidMap.get(tid);    	
    	
    	if(dirtyPages==null || dirtyPages.isEmpty()){
    		return;
    	}
    	
    	Iterator<PageId> it = dirtyPages.iterator(); 
    	
    	if(commit){
    		flushPages(tid);
    	
    	}else{    	
    		//revert any changes made by tid
    		while(it.hasNext()){
    			//replace the page in bufferpool with the corresponding page from disk
    			PageId pid = it.next();
    			if(pages.get(pid).isDirty()!=null){
    				synchronized(this){
		    		    DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
		    		    Page diskPage = f.readPage(pid); 	    		    
	    		    	pages.put(pid, diskPage);
	    		    }
    			}
    		}  
    	}
    	
    	//release locks/requests
    	it = dirtyPages.iterator();
    	while(it.hasNext()){
    		PageId pid = it.next();
	    	LockManager lm = lockManagers.get(pid);
	    	synchronized(this){
		    	lm.complete = true;		//stop waiting lock requests from acquiring lock
		    	releasePage(tid,pid);
	    	}
    	}        
    	synchronized(this){
    		tidMap.remove(tid);
    	}
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
    		Page dirtyPage = dirtyItr.next();
    		//PageId pid = dirtyPage.getId();
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
    	
    	//System.out.println(t.getRecordId().getPageId().getTableId);
    	//Database.getCatalog().
    	//t.getRecordId().
    	int tableId = t.getRecordId().getPageId().getTableId();
    	HeapFile file = (HeapFile)Database.getCatalog().getDatabaseFile(tableId);
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
		    		HeapFile f = (HeapFile)Database.getCatalog().getDatabaseFile(pid.getTableId());		    		
					f.writePage(p); 					//write page to disk					
					p.markDirty(false, p.isDirty());	//mark the page clean?	
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
        //set of pages associated with tid
    	Set<PageId> dirtyPages = tidMap.get(tid);
    	if(dirtyPages==null || dirtyPages.isEmpty()){
    		//System.out.println("no dirtyPages");
    		return;
    	}    	
    	Iterator<PageId> it = dirtyPages.iterator();    	
    	
    	//flush dirty pages associated with tid
    	while(it.hasNext()){
    		PageId pid = it.next();
    		if(pages.get(pid).isDirty()!=null){
    			//System.out.println("page is dirty");
    			flushPage(pid);
    		} 
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
    	/* 
    	 * LRU PAGE EVICTION POLICY: 
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
    	 * 
    	 * 
    	 * NO STEAL POLICY:
    	 * 
    	 * It does not evict any dirty page, and throws a DbException
    	 * if all the pages are dirty.
    	 */
    	
    	//choose page
    	Set<PageId> pa = pages.keySet();     	
    	
    	PageId pp = null;	
    	int min = 999999999;
    	
    	Iterator<PageId> it = pa.iterator();
    	while(it.hasNext()){
    		PageId i = it.next();
    		int n = accessAr.get(i);
    		//page is NOT dirty and has the lowest value in accessAr
    		if(pages.get(i).isDirty()==null && min>n){
    			min = n;
    			pp = i;
    		}
    	}
    	
    	//throw DbException if all pages are dirty
    	if(pp==null){
    		throw new DbException("all pages in the buffer are dirty");
    	}
    	
    	//evict the chosen page
    	try{
	    	flushPage(pp);
	    	pages.remove(pp);
	    	accessAr.remove(pp);
	    	lockManagers.remove(pp);
	    	Iterator<TransactionId> tidIt = tidMap.keySet().iterator();
	    	while(tidIt.hasNext()){
	    		TransactionId tid = tidIt.next();
	    		if(tidMap.get(tid).contains(pp)){
	    			tidMap.get(tid).remove(pp);
	    		}
	    	}	    	
    	}catch(IOException e){
    		throw new DbException("cannot evit page");
    	}
    } 
    
    static class LockManager {
    	private boolean inUse = false;
    	private boolean complete = false;
    	private Permissions perm = null;
    	//number of transactions that currently have lock on the page (counting upgrade and shared)
    	private int num = 0;    	
    	//keeps track of all the transactions that either acquired or are waiting for the lock
    	//boolean value true means the transaction has the lock, false means it's waiting 
    	private ConcurrentHashMap<TransactionId,Boolean> txns = new ConcurrentHashMap<TransactionId,Boolean>();
    	    	
        public void acquireLock(TransactionId tid, Permissions perm) throws TransactionAbortedException{
        	boolean waiting = true;	    		
        	//page currently not accepting any locks
    		if(complete){
    			return;
    		}    			
    		synchronized(this){
    			//not in use, therefore acquire lock
    			if(!inUse){
    				inUse = true;
    				waiting = false;
              		this.perm = perm;
              		txns.put(tid, true);
               		num++;
              		return;
    			}else{
    				//different transactions, but read only accesses
	    			if(this.perm.equals(Permissions.READ_ONLY) && perm.equals(Permissions.READ_ONLY)){
	              		waiting = false;
	              		txns.put(tid, true);
	               		num++;
	               		return;
	               	//same transaction, same permission
	    			}else if(perm.equals(this.perm) && txns.containsKey(tid) && txns.get(tid)){
		    	       	waiting = false;
		    	       	num++;
		    	       	return;
		    	    //same transaction already has read_write permission and the current request is read_only
	    			}else if(txns.containsKey(tid) && txns.get(tid) && this.perm.equals(Permissions.READ_WRITE) && perm.equals(Permissions.READ_ONLY)){
	    				waiting = false;
		    	       	num++;
		    	       	return;   			
	                //same transaction upgrading from read_only to read_write
	                }else if(txns.size()==1 && perm.equals(Permissions.READ_WRITE) && txns.containsKey(tid) && txns.get(tid) && this.perm.equals(Permissions.READ_ONLY)){
	                    waiting = false;
	                    this.perm = perm;
	                    num++;
	                    return;	                 
                   	//otherwise, must wait!
                   	}else{
                   		txns.put(tid, false);
                   	}    				    			
    			}
    		}//end of synchronized block checking if available, acquiring lock if possible, and updating queue	
    		
    		//if the transaction reaches here, it must busy wait
    		int waitTime = 0;
    		while(waiting){
    			if(complete){
    				return;
    			}
    			synchronized(this){
    	    		//not in use, therefore acquire lock
    	    		if(!inUse){
    	    			inUse = true;
    	    			waiting = false;
    	           		this.perm = perm; 
                  		txns.put(tid, true);
    	            	num++;
    	            	return;
    	    		}else{
    	    			//different transactions, but read only accesses
    	    			if(this.perm.equals(Permissions.READ_ONLY) && perm.equals(Permissions.READ_ONLY)){
    	              		waiting = false;
    	              		txns.put(tid, true);
    	               		num++;
    	               		return;
    	               	//same transaction, same permission
    	    			}else if(perm.equals(this.perm) && txns.containsKey(tid) && txns.get(tid)){
    		    	       	waiting = false;
    		    	       	num++;
    		    	       	return;
    		    	    //same transaction already has read_write permission and the current request is read_only
    	    			}else if(txns.containsKey(tid) && txns.get(tid) && this.perm.equals(Permissions.READ_WRITE) && perm.equals(Permissions.READ_ONLY)){
    	    				waiting = false;
    		    	       	num++;
    		    	       	return;   			
    	                //same transaction upgrading from read_only to read_write
    	                }else if(txns.size()==1 && perm.equals(Permissions.READ_WRITE) && txns.containsKey(tid) && txns.get(tid) && this.perm.equals(Permissions.READ_ONLY)){
    	                    waiting = false;
    	                    this.perm = perm;
    	                    num++;
    	                    return;	          
    	                }    	    			
    				}
    			}    			
    			if(waiting){
    				//if long wait, throw exception
    				if(waitTime>5){
                   		throw new TransactionAbortedException();	    			
        			}
    				//sleep for 100 milliseconds
    				try{
						Thread.sleep(100);						
					}catch(InterruptedException e){}
    				waitTime++;
    			}   			         
    		}
        }
        
        public synchronized void releaseLock(TransactionId tid){ 
        	int running = 0;
        	        	
        	//see how many transactions are running
        	Iterator<Entry<TransactionId,Boolean>> it = txns.entrySet().iterator();
        	while(it.hasNext()){
        		Entry<TransactionId,Boolean> e = it.next();
        		if(e.getValue()){
        			running++;
        		}
        	}
        	//if there are no running transactions, let waiting transactions acquire the lock
        	if(running==0){
        		inUse = false;
        	}
        	
        	if(txns.containsKey(tid)){
        		//this transaction is the only one running
	        	if(txns.get(tid) && running==1){
	        		txns.remove(tid);
	        		inUse = false;
	        		this.perm = null;
	        		num = 0;
	        	//running but with other transactions also running
	        	}else if(txns.get(tid) && running>1){
	        		txns.remove(tid);
	        		num--;	        	
	        	//transaction still waiting
	        	}else if(!txns.get(tid)){
	        		txns.remove(tid);        		
	        	}
        	}
        	complete = false;
        }
    }
}
