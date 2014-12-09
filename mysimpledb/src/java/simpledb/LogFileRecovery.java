package simpledb;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author mhay
 */
class LogFileRecovery {

    private final RandomAccessFile readOnlyLog;

    /**
     * Helper class for LogFile during rollback and recovery.
     * This class given a read only view of the actual log file.
     *
     * If this class wants to modify the log, it should do something
     * like this:  Database.getLogFile().logAbort(tid);
     *
     * @param readOnlyLog a read only copy of the log file
     */
    public LogFileRecovery(RandomAccessFile readOnlyLog) {
        this.readOnlyLog = readOnlyLog;
    }

    /**
     * Print out a human readable representation of the log
     */
    public void print() throws IOException {
        // since we don't know when print will be called, we can save our current location in the file
        // and then jump back to it after printing
        Long currentOffset = readOnlyLog.getFilePointer();

        readOnlyLog.seek(0);
        readOnlyLog.readLong();
        System.out.println("BEGIN LOG FILE");
        while (readOnlyLog.getFilePointer() < readOnlyLog.length()) {
            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();
            switch (type) {
                case LogType.BEGIN_RECORD:
                    System.out.println("<T_" + tid + " BEGIN>");
                    break;
                case LogType.COMMIT_RECORD:
                    System.out.println("<T_" + tid + " COMMIT>");
                    break;
                case LogType.ABORT_RECORD:
                    System.out.println("<T_" + tid + " ABORT>");
                    break;
                case LogType.UPDATE_RECORD:
                    Page beforeImg = LogFile.readPageData(readOnlyLog);
                    Page afterImg = LogFile.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " UPDATE pid=" + beforeImg.getId() +">");
                    break;
                case LogType.CLR_RECORD:
                    afterImg = LogFile.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " CLR pid=" + afterImg.getId() +">");
                    break;
                case LogType.CHECKPOINT_RECORD:
                    int count = readOnlyLog.readInt();
                    Set<Long> tids = new HashSet<Long>();
                    for (int i = 0; i < count; i++) {
                        long nextTid = readOnlyLog.readLong();
                        tids.add(nextTid);
                    }
                    System.out.println("<T_" + tid + " CHECKPOINT " + tids + ">");
                    break;
                default:
                    throw new RuntimeException("Unexpected type!  Type = " + type);
            }
            readOnlyLog.readLong();   // ignored, only useful when going backwards thru log
        }
        System.out.println("END LOG FILE");

        // return the file pointer to its original position
        readOnlyLog.seek(currentOffset);

    }

    /**
     * Rollback the specified transaction, setting the state of any
     * of pages it updated to their pre-updated state.  To preserve
     * transaction semantics, this should not be called on
     * transactions that have already committed (though this may not
     * be enforced by this method.)
     *
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     *
     * @param tidToRollback The transaction to rollback
     * @throws java.io.IOException if tidToRollback has already committed
     */
    public void rollback(TransactionId tidToRollback) throws IOException {
    	Long currentOffset = readOnlyLog.getFilePointer();

        readOnlyLog.seek(0);
        readOnlyLog.readLong();
        
        Set<Page> pagesToUndo = new HashSet<Page>();
        
        while(readOnlyLog.getFilePointer() < readOnlyLog.length()) {
            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();            
            //look for update records associated with the specified transaction
            switch(type) {
	            case LogType.BEGIN_RECORD:	
	            	//this if statement seems to change test results
	            	if(tid==tidToRollback.getId())
	            		Database.getLogFile().logAbort(tid);
	            	break;
	            case LogType.COMMIT_RECORD:
	            	if(tid==tidToRollback.getId()){
	            		throw new IOException("transaction already commited");
	            	}
	            	break;
	            case LogType.ABORT_RECORD:
	                break;
	            case LogType.UPDATE_RECORD:
	            	Page beforeImg = LogFile.readPageData(readOnlyLog);
	            	LogFile.readPageData(readOnlyLog); 
	            	if(tid==tidToRollback.getId()){
	            		pagesToUndo.add(beforeImg);
	            	}
	                break;
	            case LogType.CLR_RECORD:
	                LogFile.readPageData(readOnlyLog); 
	                break;
	            case LogType.CHECKPOINT_RECORD:
	                int count = readOnlyLog.readInt();
	                readOnlyLog.seek(readOnlyLog.getFilePointer()+count*LogFile.LONG_SIZE);
	                break;
	            default:
	                throw new RuntimeException("Unexpected type!  Type = " + type);            
            }
            readOnlyLog.readLong(); 
        }
        //return the file pointer to its original position
        readOnlyLog.seek(currentOffset);    
        
        //go through the set of pages that need to be undone in reverse order and undo the changes
        Page[] undoArr = new Page[pagesToUndo.size()];
        pagesToUndo.toArray(undoArr);
        int i = undoArr.length-1;
        for(;i>=0;i--){
        	Page p = undoArr[i];
        	PageId pid = p.getId();
        	int tableId = pid.getTableId();
        	Database.getCatalog().getDatabaseFile(tableId).writePage(p);
        	Database.getBufferPool().discardPage(pid);
        }
    }

    /**
     * Recover the database system by ensuring that the updates of
     * committed transactions are installed and that the
     * updates of uncommitted transactions are not installed.
     *
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     */
    public void recover() throws IOException {    	
    	Long currentOffset = readOnlyLog.getFilePointer();
    	
    	/*
    	 * Read the last checkpoint, if any.
    	 */    	
        readOnlyLog.seek(0);
        long lastCheckpoint = readOnlyLog.readLong();
        if(lastCheckpoint==-1){
        	lastCheckpoint = LogFile.LONG_SIZE;
        }       
            	
    	/*
    	 * Scan forward from the checkpoint (or start of log file) to build the set of loser transactions
    	 * Re-do updates during this pass
    	 */
        Set<Long> losers = new HashSet<Long>();
        readOnlyLog.seek(lastCheckpoint);
        while (readOnlyLog.getFilePointer() < readOnlyLog.length()) {
            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();            
            switch (type) {
                case LogType.BEGIN_RECORD:
                	losers.add(tid);
                    break;
                case LogType.COMMIT_RECORD:
                case LogType.ABORT_RECORD:
                	if(losers.contains(tid)){
                		losers.remove(tid);
                	}
                    break;
                case LogType.UPDATE_RECORD:
                    LogFile.readPageData(readOnlyLog);
                    Page afterImg = LogFile.readPageData(readOnlyLog);                   
	                int tableId = afterImg.getId().getTableId();
	                Database.getCatalog().getDatabaseFile(tableId).writePage(afterImg);
	                break;
                case LogType.CLR_RECORD:
                    afterImg = LogFile.readPageData(readOnlyLog);
                    break;
                case LogType.CHECKPOINT_RECORD:
                    int count = readOnlyLog.readInt();
                    readOnlyLog.seek(readOnlyLog.getFilePointer()+count*LogFile.LONG_SIZE);
	                break;
                default:
                    throw new RuntimeException("Unexpected type!  Type = " + type);
            }
            //long startOfRecord = readOnlyLog.readLong();
            readOnlyLog.seek(readOnlyLog.getFilePointer()+LogFile.LONG_SIZE);
            
        }
        
        
        /*
         * Un-do the updates of loser transactions
         */
        long endOfLog = readOnlyLog.getFilePointer();
        if(losers.isEmpty()){}
        else{
        	Iterator<Long> it = losers.iterator();
        	while(it.hasNext()){
        		readOnlyLog.seek(endOfLog);
        		TransactionId tid = new TransactionId(it.next());
        		rollback(tid);
        	}
        }   	

        
        //return the file pointer to its original position
        readOnlyLog.seek(currentOffset); 
    }
}
