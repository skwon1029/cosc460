package simpledb;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {	
	
    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
        
        public boolean equals(TDItem other){
        	if(other==null){
        		return false;
        	}
        	if(this.fieldName.equals(other.fieldName)&&this.fieldType.equals(other.fieldType)){
        		return true;
        	}
        	return false;
        }
    }

    private static final long serialVersionUID = 1L;
    
    /**
     * Array of TDItems inside TupleDesc
     */
    private TDItem[] TDAr;
    
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        int len = typeAr.length;
        TDAr = new TDItem[len];        
        for(int i=0;i<len;i++){
        	TDAr[i] = new TDItem(typeAr[i],fieldAr[i]);
        }        
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        int len = typeAr.length;
        TDAr = new TDItem[len];
        for(int i=0;i<len;i++){
        	TDAr[i] = new TDItem(typeAr[i],null);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return TDAr.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	//if i is a valid field reference
    	if(i>=0 && i<numFields()){
    		return TDAr[i].fieldName;
    	} throw new NoSuchElementException("field reference is invalid");
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	//if i is a valid field reference
    	if(i>=0 && i<numFields()){
    		return TDAr[i].fieldType;
    	} throw new NoSuchElementException("field reference is invalid");
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	if(name==null){
    		throw new NoSuchElementException("null is not a valid field name");
    	}
    	for(int i=0;i<numFields();i++){
    		if(name.equals(getFieldName(i))){
    			return i;
    		}
    	} 
    	throw new NoSuchElementException("no field with a matching name is found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int size = 0;
    	for(int i=0;i<numFields();i++){
    		//add byte size of each type
    		size += getFieldType(i).getLen();    			
    	}
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	//total length of the merged TupleDesc
        int total = td1.numFields()+td2.numFields();
        
        //initialize input arrays for the merged TupleDesc
        Type[] newTypeAr = new Type[total];
        String[] newNameAr = new String[total];
        
        //index for the merged TupleDesc
        int index = 0;
        
        //copy td1 and td2 to the new TupleDesc
        for(int i=0; i<td1.numFields();i++){
        	newTypeAr[index]=td1.getFieldType(i);
        	newNameAr[index++]=td1.getFieldName(i);
        }
        for(int j=0; j<td2.numFields();j++){
        	newTypeAr[index]=td2.getFieldType(j);
        	newNameAr[index++]=td2.getFieldName(j);
        }
        return new TupleDesc(newTypeAr,newNameAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	//return false if the specified object is null
    	if(o == null){
    		return false;
    	}
    	
    	//return false if the object cannot be converted TupleDesc
    	TupleDesc other;
    	try{
    		other = (TupleDesc) o;
    	} catch(ClassCastException e){
    		return false;
    	}   
    	
    	//return false if the sizes differ
    	if(this.getSize()!=other.getSize()){
    		return false;
    	}
    	//return false if the types differ
    	for(int i=0;i<numFields();i++){
    		if(!this.getFieldType(i).equals(other.getFieldType(i))){
    			return false;
    		}
    	}    	
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldName[0](fieldType[0]), ..., fieldName[M](fieldType[M])"
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        String result = "";
        for(int i=0;i<numFields();i++){
        	result += TDAr[i].toString();
        	if(i!=numFields()-1){
        		result += ", ";
        	}
        }        
        return result;
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return Arrays.asList(TDAr).iterator();
    }

}
