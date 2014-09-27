package simpledb;

import java.io.IOException;

public class Lab3Main {

    public static void main(String[] argv) 
       throws DbException, TransactionAbortedException, IOException {

        System.out.println("Loading schema from file:");
        // file named college.schema must be in mysimpledb directory
        Database.getCatalog().loadSchema("college.schema");
        
        // SQL query: SELECT * FROM STUDENTS S, TAKES T WHERE S.sid=T.tid
        // - a Join operator is the root; join keeps only those w/ S.sid = T.sid
        // - a SeqScan operator on Students at the child of root
        // - a SeqScan operator on Takes at the child of root
        TransactionId tid = new TransactionId();
        SeqScan scanStudents = new SeqScan(tid, Database.getCatalog().getTableId("students"));
        SeqScan scanTakes = new SeqScan(tid, Database.getCatalog().getTableId("takes"));
        JoinPredicate p = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
        Join result = new Join(p, scanStudents, scanTakes);

        // query execution: we open the iterator of the root and iterate through results
        System.out.println("Query results:");
        result.open();
        while (result.hasNext()) {
            Tuple tup = result.next();
            System.out.println("\t"+tup);
        }
        result.close();
        Database.getBufferPool().transactionComplete(tid);
    }

}