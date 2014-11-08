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
        int offset = BufferPool.getPageSize() * page.getId().pageNumber();
        byte[] data = page.getPageData();
        if(data.length<=BufferPool.getPageSize())
        try{
	        RandomAccessFile file = new RandomAccessFile(f,"rw");
	        file.write(data); //file.write(data,offset,data.length);
		    file.close();
        }catch(IOException e){
        	throw new IOException("cannot write page");
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
        	p = (HeapPage)buffer.getPage(tid, pid, null);
        	
        	//if the page has space, insert tuple
        	if(p.getNumEmptySlots()!=0){        		
        		p.insertTuple(t);
        		result.add(p);
        		return result;
        	}
        }
        //if there aren't any pages with space
        //create a new page in the file
        byte[] newPageData = HeapPage.createEmptyPageData();
        HeapPageId newPid = new HeapPageId(tableId,numPages());
        HeapPage newPage = new HeapPage(newPid,newPageData);
        
        //insert tuple to the new page
        newPage.insertTuple(t);        
        newPageData = newPage.getPageData();
        
        //add the new page to file      
        try{ 
        	OutputStream output = new BufferedOutputStream(new FileOutputStream(f,true));
	        output.write(newPageData);
	        output.close();
	        result.add(newPage);
	        return result;
        } catch(IOException e){
        	throw new IOException("cannot add new page");
        }      
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	ArrayList<Page> result = new ArrayList<Page>();
        RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();
        BufferPool buffer = Database.getBufferPool();
        HeapPage p = null;
        p = (HeapPage)buffer.getPage(tid, pid, null);
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
    		public void open(){
				open = true;
    		}
    		@Override
    		public boolean hasNext(){     			
    			//return false if the iterator hasn't been opened
    			if(open==false){
    				return false;
    			}   			
    			//set current page and its iterator if it hasn't been set up
    			if(heapItr==null){
    				try {
						h = (HeapPage)buffer.getPage(t,pid,null);
					} catch (TransactionAbortedException e) {
						e.printStackTrace();
					} catch (DbException e) {
						e.printStackTrace();
					}
					heapItr = h.iterator();
					readPages = 1;
    			}    			
    			//return true if there are tuples left in current page 
    			if(heapItr.hasNext()){
    				return true;
    			}   			
    			//return false if current page is the last page
    			if(readPages==numPages()){
    				return false;
    			}    			
    			//skip empty pages and see if there are any tuples
    			while(readPages<numPages()){
    				try {
    					pid = new HeapPageId(tableId,pid.pageNumber()+1);
    					h = (HeapPage)buffer.getPage(t,pid,null);
    					readPages++;
    					heapItr = h.iterator();    					
    					if(heapItr.hasNext()){
    						return true;
    					}
    				} catch (TransactionAbortedException e){
    					e.printStackTrace();
    				} catch (DbException e){
    					e.printStackTrace();					
    				}
    			}
    			return false;    			
    		}
    		
    		@Override
    		public Tuple next(){
    			if(!hasNext()){
    				throw new NoSuchElementException();
    			}
    			Tuple result;
    			if(heapItr.hasNext()){
    				result=heapItr.next();
    				return result;
    			}
	    		throw new NoSuchElementException();
    		}
    		
    		@Override
    		public void rewind(){
    			h = null;
    			heapItr = null;
    			readPages = 0;
				pid = new HeapPageId(tableId,0);
    		}
    		
    		@Override
    		public void close(){
    			h = null;
    			heapItr = null;
    			open = false;
    			readPages = 0;
    		}
    	}     
    	DbFileIterator result = new tempIterator();
        return result;
    }
}

