package concurrency_tutorial;

public class ZigZagThreads {
    private static final LockManager lm = new LockManager();
    public static LockManager getLockManager() { return lm; }

    public static void main(String args[]) throws InterruptedException {
        int numZigZags = 10;
        for (int i = 0; i < numZigZags; i++) {
            new Thread(new Zigger()).start();
        }
        for (int i = 0; i < numZigZags; i++) {
            new Thread(new Zagger()).start();
        }
    }

    static class Zigger implements Runnable {

        protected String myPattern;
        protected boolean isZigger;

        public Zigger() {
            myPattern = "//////////";
            isZigger = true;
        }

        public void run() {
            ZigZagThreads.getLockManager().acquireLock(isZigger);
            System.out.println(myPattern);
            ZigZagThreads.getLockManager().releaseLock();
        }
    }

    static class Zagger extends Zigger {

        public Zagger() {
            myPattern = "\\\\\\\\\\\\\\\\\\\\";
            isZigger = false;
        }

    }

    static class LockManager {
        private boolean inUse = false;
        private boolean needZig = true;

        private synchronized boolean isLockFree(boolean isZigger) {
            if(!inUse){
            	return true;
            }
            return false;
        }

        public synchronized void acquireLock(boolean isZigger) {
        	 boolean waiting = true;
             while(waiting){          		
             	if(isLockFree(isZigger) && needZig==isZigger){            		
             		inUse = true;
             		waiting = false;
             	}else{
             		try {
 						wait();
 					} catch (InterruptedException e) {}
             	}				
             }
        }

        public synchronized void releaseLock() {
        	inUse = false;
        	if(needZig){
        		needZig = false;
        	}else{
        		needZig = true;
        	}
            notifyAll(); //once done, notify threads that are waiting
        }
    }}

