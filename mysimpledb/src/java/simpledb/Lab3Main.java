package simpledb;

import java.io.IOException;

public class Lab3Main {

    public static void main(String[] argv) 
       throws DbException, TransactionAbortedException, IOException {

        System.out.println("Loading schema from file:");
        // file named college.schema must be in mysimpledb directory
        Database.getCatalog().loadSchema("college.schema");
        
        /*
        SELECT S.name
		FROM Students S, Takes T, Profs P
		WHERE	S.sid = T.sid AND
      			T.cid = P.favoriteCourse AND
      			P.name = "hay"
         */
        TransactionId tid = new TransactionId();
        SeqScan scanStudents = new SeqScan(tid, Database.getCatalog().getTableId("students"));
        SeqScan scanTakes = new SeqScan(tid, Database.getCatalog().getTableId("takes"));
        SeqScan scanProfs = new SeqScan(tid, Database.getCatalog().getTableId("profs"));
        JoinPredicate p = new JoinPredicate(0, Predicate.Op.EQUALS, 0);	//S.sid = T.sid
        JoinPredicate p2 = new JoinPredicate(1,Predicate.Op.EQUALS,2);	//T.cid = P.favorateCourse
        StringField hay = new StringField("hay",Type.STRING_LEN); 
        Predicate p3 = new Predicate(1,Predicate.Op.EQUALS,hay);		//P.name = "hay"
        
        Filter filterProfs = new Filter(p3, scanProfs);
        Join joinTakesProfs = new Join(p2, scanTakes, filterProfs);
        Join joinStudentsTakes = new Join(p, scanStudents, joinTakesProfs);
        
        // query execution: we open the iterator of the root and iterate through results
        System.out.println("Query results:");
        joinStudentsTakes.open();
        while (joinStudentsTakes.hasNext()) {
            Tuple tup = joinStudentsTakes.next();
            Field f = tup.getField(1);
            System.out.println("\t"+f);
        }
        joinStudentsTakes.close();
        Database.getBufferPool().transactionComplete(tid);
    }

}