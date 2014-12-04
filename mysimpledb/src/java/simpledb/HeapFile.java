package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

	private File f;
	private final TupleDesc td;
	private final int tableId;
	
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f=f;
        this.td=td;
        this.tableId=f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {   
        int pageSize = BufferPool.getPageSize();
        int offset = pid.pageNumber() * pageSize;

        //byte array to temporarily store data from input stream
        byte[] data = new byte[pageSize];
        
        //open the file and return page
        try{        	
        	FileInputStream input = new FileInputStream(f);
        	BufferedInputStream buffInput = new BufferedInputStream(input);
        	buffInput.skip(offset);
        	buffInput.read(data);
        	Page result = new HeapPage((HeapPageId)pid,data);
        	buffInput.close();
        	return result;
        }catch(IOException e){        	
        	return null;
        }
    }

    public void writePage(Page page) throws IOException {
        int offset =  page.getId().pageNumber() * BufferPool.getPageSize();
        byte[] data = page.getPageData();
        if(data.length<=BufferPool.getPageSize()){
	        try{
		        RandomAccessFile file = new RandomAccessFile(f,"rw");
		        file.seek((long)offset);
		        file.write(data);
		        file.close();
	        }catch(IOException e){
	        	throw new IOException("cannot write page");
	        }
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        long fileSize = f.length();
        int pageSize = BufferPool.getPageSize();
        return (int)fileSize/pageSize;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	ArrayList<Page> result = new ArrayList<Page>(); //arraylist to return
    	BufferPool buffer = Database.getBufferPool();
    	HeapPageId pid = null;
    	HeapPage p = null;
    	
    	//go through pages and find empty slot
        for(int i=0;i<numPages();i++){
        	pid = new HeapPageId(tableId,i);
        	p = (HeapPage)buffer.getPage(tid, pid, Permissions.READ_ONLY);
        	
        	//if the page has space, insert tuple
        	if(p.getNumEmptySlots()!=0){
	        	//upgrade permission
        		p = (HeapPage)buffer.getPage(tid, pid, Permissions.READ_WRITE);
        		p.insertTuple(t);        		
	        	result.add(p);  
	        	return result;
        	}
	        //release lock if we just acquired the lock
        	if(buffer.numTransactions(pid)==1){
        		buffer.releasePage(tid, pid);        	
        	}
        }
        synchronized(this){
		    //if there aren't any pages with space create a new page
		    byte[] newPageData = HeapPage.createEmptyPageData();
		    HeapPageId newPid = new HeapPageId(tableId,numPages());
		    
		    //write the page to disk
		    try{ 
		    	OutputStream output = new BufferedOutputStream(new FileOutputStream(f,true));
			    output.write(newPageData);
			    output.flush();
			    output.close();			    
		    } catch(IOException e){
		     	throw new IOException("cannot add new page");
		    }  		    
		    //insert tuple to the new page
		    p = (HeapPage)buffer.getPage(tid, newPid, Permissions.READ_WRITE);
		    p.insertTuple(t); 
		    result.add(p);
		    return result;
        }
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	ArrayList<Page> result = new ArrayList<Page>();
        RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();
        BufferPool buffer = Database.getBufferPool();
        HeapPage p = (HeapPage)buffer.getPage(tid, pid, Permissions.READ_WRITE);
        p.deleteTuple(t);
        result.add(p);        
        return result;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	final TransactionId t = tid;    	
    	class tempIterator implements DbFileIterator{
    		
    		BufferPool buffer;
    		HeapPageId pid; 		
    		HeapPage h = null;
    		boolean open = false;
    		Iterator<Tuple> heapItr;	//heap iterator
    		int readPages = 0; 			//keeps track of the number of pages read
			
			public tempIterator(){
				buffer = Database.getBufferPool();
				pid = new HeapPageId(tableId,0);
			}
			
			@Override
    		public void open() throws DbException, TransactionAbortedException{
				open = true;
    		}
    		@Override
    		public boolean hasNext() throws DbException, TransactionAbortedException{     			
    			//return false if the iterator hasn't been opened
    			if(open==false){
    				return false;
    			}   			
    			//set current page and its iterator if it hasn't been set up
    			if(heapItr==null){
					h = (HeapPage)buffer.getPage(t,pid,Permissions.READ_ONLY);
					heapItr = h.iterator();
					readPages = 1;				
    			}    			
    			
    			//return true if there are tuples left in current page 
    			if(heapItr.hasNext()){
    				return true;
    			}   			
    			//return false if current page is the last page
    			if(readPages==numPages()){
    				//release lock if we just acquired the lock
    	        	if(buffer.numTransactions(pid)==1){
    	        		buffer.releasePage(t, pid);        	
    	        	}
    				return false;
    			}    			
    			//skip empty pages and see if there are any tuples
    			while(readPages<numPages()){
    				buffer.releasePage(t, pid);
    				pid = new HeapPageId(tableId,pid.pageNumber()+1);
    				h = (HeapPage)buffer.getPage(t,pid,Permissions.READ_ONLY);
    				readPages++;
    				heapItr = h.iterator(); 
    				if(heapItr.hasNext()){
    					return true;
    				}
    				if(buffer.numTransactions(pid)==1){
    	        		buffer.releasePage(t, pid);        	
    	        	}
    			}
    			return false;    			
    		}
    		
    		@Override
    		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException{
    			if(!hasNext()){
    				throw new NoSuchElementException();
    			}
    			Tuple result;
    			if(heapItr.hasNext()){
    				result=heapItr.next();
    				if(buffer.numTransactions(pid)==1){
    	        		buffer.releasePage(t, pid);        	
    	        	}
    				return result;
    			}
    			
	    		throw new NoSuchElementException();
    		}
    		
    		@Override
    		public void rewind() throws DbException, TransactionAbortedException{
    			buffer = Database.getBufferPool();
    			pid = new HeapPageId(tableId,0);
    			h = null;
    			heapItr = null;
    			readPages = 0;
    			//buffer.releasePage(t, pid);
							
    		}
    		
    		@Override
    		public void close(){
    			buffer = null;
    			pid = null;
    			h = null;
    			heapItr = null;
    			open = false;
    			readPages = 0;
    			//buffer.releasePage(t, pid);
    		}
    	}     
    	DbFileIterator result = new tempIterator();
        return result;
    }
}
