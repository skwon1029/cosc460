package simpledb;
import java.io.*;

public class Lab2Main {
	
    public static void main(String[] argv) {
    	
    	//(some parts copied from Lab1Main.java)
        //construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2" };
        TupleDesc descriptor = new TupleDesc(types, names);

        //create the table, associate it with some_data_file.dat
        //and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("some_data_file.dat"), descriptor);
        Database.getCatalog().addTable(table1, "test");
        
        //construct the query: we use a simple SeqScan, which spoonfeeds
        //tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());
        
        try {
            f.open();            
            while (f.hasNext()) {
                Tuple tup = f.next();
                IntField three = new IntField(3); //integer 3
                //1. find all records whose field1 value is less than 3
                if(tup.getField(1).compare(Predicate.Op.LESS_THAN, three)){
                	System.out.print("Update tuple: "+tup);
                	Database.getBufferPool().deleteTuple(tid, tup);
                	tup.setField(1, three);
                	//update the value to 3
                	Database.getBufferPool().insertTuple(tid, table1.getId(), tup);
                	System.out.println(" to be: "+tup);
                }
            }
            
            //2. insert a new tuple with values 99, 99, and 99
            Tuple newTup = new Tuple(descriptor);
            IntField nintynine = new IntField(99);  //integer 99           
            for(int i=0;i<3;i++){
            	newTup.setField(i, nintynine);
            }
            Database.getBufferPool().insertTuple(tid, table1.getId(), newTup);
            System.out.println("Insert tuple: "+newTup);
            
            //3. print out all the tuples
            f.rewind();
            System.out.println("The table now contains the following records:");
            while (f.hasNext()){
            	Tuple tup = f.next();
            	System.out.println("Tuple "+tup);
            } 
            
            //flush changed data to disk
            Database.getBufferPool().flushAllPages();
            
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println ("Exception : " + e);
        }
    }
}