package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Vector;

import simpledb.Predicate.Op;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p/>
 * This class is not needed in implementing lab1|lab2|lab3.                                                   // cosc460
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(HashMap<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    private Vector<Object> histVec = new Vector<Object>();
    private Vector<Object> distVec = new Vector<Object>();
    private DbFile f = null;
    private int ioCostPerPage = 0;
    private int numFields = 0;
    private int numTups = 0;
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {    	    	
    	this.ioCostPerPage = ioCostPerPage;
    	f = Database.getCatalog().getDatabaseFile(tableid);
    	numFields = f.getTupleDesc().numFields();
    	
    	Vector<Field> minArr = new Vector<Field>();
    	Vector<Field> maxArr = new Vector<Field>();
    	
    	DbFileIterator it = f.iterator(null);
    	try{
			it.open();
		}catch(DbException|TransactionAbortedException e){
			e.printStackTrace();
		}
    	
    	//compute the minimum and maximum values for each field
    	try{
			if(it.hasNext()){
				Tuple t = it.next();
				numTups += 1;
				for(int i=0; i<numFields;i++){
					minArr.add(t.getField(i));
					maxArr.add(t.getField(i));    			
				}    		
			}
		}catch(NoSuchElementException|DbException|TransactionAbortedException e){
			e.printStackTrace();
		}   	
    	try{
			while(it.hasNext()){
				Tuple t = it.next();
				numTups += 1;
				for(int i=0; i<numFields; i++){
					Field v = t.getField(i);
					if(v.compare(Predicate.Op.LESS_THAN, minArr.get(i))){
						minArr.set(i,v);
					}
					else if(v.compare(Op.GREATER_THAN, maxArr.get(i))){
						maxArr.set(i,v);
					}
				}    		
			}
		}catch(NoSuchElementException | DbException | TransactionAbortedException e){
			e.printStackTrace();
		}
    	
    	//create histogram for each field
    	for(int i=0; i<numFields;i++){
    		Type t = f.getTupleDesc().getFieldType(i);
    		if(t.equals(Type.INT_TYPE)){
    			int min = ((IntField)minArr.get(i)).getValue();
    			int max = ((IntField)maxArr.get(i)).getValue();
    			IntHistogram his = new IntHistogram(NUM_HIST_BINS,min,max);
    			IntHistogram distHis = new IntHistogram(max-min+1,min,max);
    			histVec.add(his);
    			distVec.add(distHis);
    		}else{
    			StringHistogram his = new StringHistogram(NUM_HIST_BINS);
    			histVec.add(his);
    			distVec.add(his);
    		}
    	}
    	
    	//add values to the histograms
    	try{
			it.rewind();
		}catch(DbException|TransactionAbortedException e) {
			e.printStackTrace();
		}
    	try{
			while(it.hasNext()){
				Tuple t = it.next();
				for(int i=0; i<numFields; i++){
					Field v = t.getField(i);
					if(v.getType().equals(Type.INT_TYPE)){
						((IntHistogram)histVec.get(i)).addValue(((IntField)v).getValue());
						((IntHistogram)distVec.get(i)).addValue(((IntField)v).getValue());
					}else{
						((StringHistogram)histVec.get(i)).addValue(((StringField)v).getValue());
						((StringHistogram)distVec.get(i)).addValue(((StringField)v).getValue());
					}
				}    		
			}
		}catch(NoSuchElementException | DbException | TransactionAbortedException e){
			e.printStackTrace();
		} 
    	it.close();
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p/>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost(){
        return ((HeapFile)f).numPages() * this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {    	
    	if(selectivityFactor>0 && selectivityFactor<1/numTups){
    		return 1;    		
    	}
    	return (int)(Math.ceil(numTups * selectivityFactor));
    }

    /**
     * This method returns the number of distinct values for a given field.
     * If the field is a primary key of the table, then the number of distinct
     * values is equal to the number of tuples.  If the field is not a primary key
     * then this must be explicitly calculated.  Note: these calculations should
     * be done once in the constructor and not each time this method is called. In
     * addition, it should only require space linear in the number of distinct values
     * which may be much less than the number of values.
     *
     * @param field the index of the field
     * @return The number of distinct values of the field.
     */
    public int numDistinctValues(int field) {
    	int result = 0;
        if(f.getTupleDesc().getFieldType(field).equals(Type.INT_TYPE)){
        	int arr[] = ((IntHistogram)distVec.get(field)).getBuckets();
        	for(int i=0; i<arr.length; i++){
        		if(arr[i]>0){
        			result += 1;
        		}
        	}
        }
        return result;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if(constant.getType().equals(Type.INT_TYPE)){
        	IntHistogram his = (IntHistogram)histVec.get(field);
        	return his.estimateSelectivity(op, ((IntField)constant).getValue());        	
        }else{
        	StringHistogram his = (StringHistogram)histVec.get(field);
        	return his.estimateSelectivity(op, ((StringField)constant).getValue());
        }
    }

}
