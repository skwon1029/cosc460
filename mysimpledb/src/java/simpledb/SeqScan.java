package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private final int tableid;
    private final String tableAlias;
    private final DbFile f;
    private final DbFileIterator it;
    
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid=tid;
        this.tableid=tableid;
        this.tableAlias=tableAlias;
        this.f=Database.getCatalog().getDatabaseFile(tableid);
        this.it=f.iterator(tid);
    }

    /**
     * @return return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        return tableAlias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        it.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc orig = Database.getCatalog().getTupleDesc(tableid);
        TupleDesc result;
        Type[] resultType = new Type[orig.numFields()];			//new array of field types
        String[] resultField = new String[orig.numFields()];	//new array of field names
        Iterator<TupleDesc.TDItem> it = orig.iterator();
        int i=0;
        //go through all field names, prefix them with tableAlias, add them to resultField
        while(it.hasNext()){
        	TupleDesc.TDItem temp = it.next();
        	String s = tableAlias+"."+temp.fieldName;
        	resultType[i]=temp.fieldType;
        	resultField[i++]=s;
        }
        result = new TupleDesc(resultType,resultField);
        return result;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return it.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        return it.next();
    }

    public void close() {
        it.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        it.rewind();
    }
}