package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private DbIterator child;
    private boolean delete;	//whether we called fetchNext()
    private BufferPool buffer;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.t=t;
        this.child=child;
        this.delete=false;
        this.buffer=Database.getBufferPool();
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	//return null if fetchNext() was called before
    	if(delete){
    		return null;
    	}
    	int num = 0; //number of tuples
        while(child.hasNext()){
        	Tuple temp = child.next();
        	try{
        		buffer.deleteTuple(t, temp);
        		num++;
        	}catch(IOException e){
        		throw new DbException("cannot delete tuple");
        	}  	
        }
        Tuple result = new Tuple(getTupleDesc());
        IntField f = new IntField(num);
        result.setField(0, f);
        delete = true; 
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
