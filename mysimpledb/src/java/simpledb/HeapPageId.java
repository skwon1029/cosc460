package simpledb;

/**
 * Unique identifier for HeapPage objects.
 */
public class HeapPageId implements PageId {

	public int tableId;
	public int pgNo;
	
    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo    The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        this.tableId=tableId;
        this.pgNo=pgNo;        
    }

    /**
     * @return the table associated with this PageId
     */
    public int getTableId() {
        return tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     * this PageId
     */
    public int pageNumber() {
        return pgNo;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     * the table number and the page number (needed if a PageId is used as a
     * key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
    	//concatenate table number and page number
    	String result = Integer.toString(tableId) + Integer.toString(pgNo);
    	Long l = Long.parseLong(result);	//ensure the hash code does not exceed the maximum int value
    	return l.hashCode(); 
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     * ids are the same)
     */
    public boolean equals(Object o) {
    	//return false if the specified object is null
        if(o==null){
        	return false;
        }
        
        //return false if the object cannot be converted HeapPageId
        HeapPageId other;
        try{
        	other = (HeapPageId) o;
        } catch(ClassCastException e){
        	return false;
        }
        //return false if table id or page number differ
        if(this.tableId!=other.tableId){
        	return false;
        }
        if(this.pgNo!=other.pgNo){
        	return false;
        }
        return true;
    }

    /**
     * Return a representation of this object as an array of
     * integers, for writing to disk.  Size of returned array must contain
     * number of integers that corresponds to number of args to one of the
     * constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = pageNumber();

        return data;
    }

}
