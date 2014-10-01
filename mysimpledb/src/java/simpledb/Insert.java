package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private DbIterator child;
    private int tableid;
    private boolean insert; //whether we called fetchNext()
    private BufferPool buffer;
    
    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        this.t=t;
        this.child=child;
        this.tableid=tableid;
        this.insert=false;
        this.buffer = Database.getBufferPool();
    }

    public TupleDesc getTupleDesc() {
    	Type[] typeAr = {Type.INT_TYPE};
        TupleDesc t = new TupleDesc(typeAr);        
        return t;
        
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
    }

    public void close() {
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	//return null if fetchNext() was called before
    	if(insert){
    		return null;
    	}
    	int num = 0; //number of tuples 
        while(child.hasNext()){
        	Tuple temp = child.next();
        	try{
        		buffer.insertTuple(t, tableid, temp);
        		num++;
        	}catch(IOException e){
        		throw new DbException("cannot insert tuple");
        	}  	
        }
        Tuple result = new Tuple(getTupleDesc());
        IntField f = new IntField(num);
        result.setField(0, f);
        insert = true;
        return result;
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator[] result = {child};
        return result;
    }

    @Override
    public void setChildren(DbIterator[] children) {
    	if(children.length==1){
    		child=children[0];
    	}        
    }
}
