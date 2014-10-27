package simpledb;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	private int buckets;		//number of buckets
	private int min;
	private int max;
	private int[] arr;			//numbers of values that fall into buckets (length: buckets)
	private int[] range_arr;	//boundary values for buckets (length: buckets+1)
	private int width;			
	private int last_width;		//width of the last bucket
	private int tupNum;			//total number of integer values inserted
	
    /**
     * Create a new IntHistogram.
     * <p/>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p/>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p/>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.buckets=buckets;
    	this.min=min;
        this.max=max;
        this.tupNum=0;
        this.width = (max-min+1)/buckets;
        //if the number of buckets is too big, adjust width and number of buckets
        if((max-min)<buckets){
        	this.width = 1;
        	this.buckets = max-min+1;         	
        }   
        this.arr=new int[buckets];        
        this.range_arr=new int[buckets+1];
        for(int i=0; i<buckets; i++){
        	this.range_arr[i] = min + i*width;        	
        }
        this.range_arr[buckets]=max;
        this.last_width = (max+1)-range_arr[buckets-1];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	//value v is out of range
        if(v>max || v<min){
        	throw new RuntimeException();
        }
        for(int i=0; i<buckets-1; i++){
        	if(v>=range_arr[i] && v<range_arr[i+1]){
        		arr[i] += 1;
        		tupNum += 1;
        		return;
        	}
        }
        //value v falls into the last bucket
        if(v<=max){
        	arr[buckets-1] += 1;
        	tupNum += 1;
        	return;
        }        
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p/>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	//figure out which buckets value v fall into
    	int b = 0;
    	if(v==max){
        	b = buckets-1;
        } 
    	else{
	    	for(int i=0; i<buckets; i++){
	        	if(v>=range_arr[i] && v<range_arr[i+1]){
	        		b = i;
	        		break;
	        	}
	        }
    	}
    	
    	if(op.equals(Predicate.Op.EQUALS)){
    		if(v>max || v<min){
    			return 0.0;
    		}
    		if(b==buckets-1){
    			return (double)arr[b]/last_width/tupNum;
    		}
    		return (double)arr[b]/width/tupNum;    			
    		
    	}
    	else if(op.equals(Predicate.Op.NOT_EQUALS)){
    		return 1.0-estimateSelectivity(Predicate.Op.EQUALS,v);
    	}
    	
    	else if(op.equals(Predicate.Op.GREATER_THAN)){
    		if(v<min){
    			return 1.0;
    		}
    		if(v>=max){
    			return 0.0;
    		}
    		double fraction = (double)(range_arr[b+1]-1-v)/width; 
    		double result = fraction*arr[b];
    		for(int i = b+1; i<buckets; i++){
    			result += (double)arr[i];    			
    		}
    		return result/tupNum;    		
    	}
    	else if(op.equals(Predicate.Op.LESS_THAN_OR_EQ)){
    		return 1.0-estimateSelectivity(Predicate.Op.GREATER_THAN,v);
    	}
    	
    	else if(op.equals(Predicate.Op.GREATER_THAN_OR_EQ)){
    		if(v<=min){
    			return 1.0;
    		}
    		if(v>max){
    			return 0.0;
    		}
    		double fraction = (double)(range_arr[b+1]-v)/width; 
    		//if value v falls into the last bucket
    		if(b==buckets-1){
    			fraction = (max+1-v)/width;
    		}    		
    		double result = fraction*arr[b];
    		for(int i = b+1; i<buckets; i++){
    			result += (double)arr[i];    			
    		}
    		return result/tupNum;     		
    	}
    	else if(op.equals(Predicate.Op.LESS_THAN)){
    		return 1.0-estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ,v);
    	}
    	
        return -1.0;        
    }

    /*
     * Returns the integer array that stores the numbers of values that fall into each bucket
     */
    public int[] getBuckets(){
    	return arr;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        String result = "";
        result += "Number of buckets:\t"+buckets +"\n";
        result += "Bucket width:\t"+width +"\n";
        for(int i=0; i<buckets;i++){
        	result += "Bucket "+i+" has "+arr[i]+" values\n";
        }
        return result;
    }
}
