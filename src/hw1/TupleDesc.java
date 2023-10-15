//Sushant Ramesh
package hw1;
import java.util.*;

public class TupleDesc {

	private Type[] types;
	private String[] fields;
	
    
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	this.types = typeAr;
    	this.fields = fieldAr;
    }

    public int numFields() {
        return types.length;
    	
    }

    public String getFieldName(int i) throws NoSuchElementException {
        if(i<0 || i>=fields.length) {
        	throw new NoSuchElementException();
        }
        return fields[i];
    }

    public int nameToId(String name) throws NoSuchElementException {
    	for (int i = 0; i < fields.length; i++) {
            if (fields[i].equals(name)) return i;
        }
        throw new NoSuchElementException();
    }

  
    public Type getType(int i) throws NoSuchElementException {
    	if(i<0 || i>=types.length) {
        	throw new NoSuchElementException();
        }
        return types[i];
    }

   
    public int getSize() {
    	int aggregateSize = 0;
        for (int i = 0; i < types.length; i++) {
            aggregateSize += (types[i] == Type.INT) ? 4 : 129; 
        }
        return aggregateSize;
    }

   
    public boolean equals(Object obj) {
    	if (this == obj) return true;  
        if (!(obj instanceof TupleDesc)) return false;  
        
        TupleDesc other = (TupleDesc) obj;
        
        if (this.getSize() != other.getSize()) return false;          
        for (int i = 0; i < types.length; i++) {
            if (this.getType(i) != other.getType(i)) return false;  
        }
        
        return true;
    }
    

    public int hashCode() {
        throw new UnsupportedOperationException("unimplemented");
    }

   
    public String toString() {
    	StringBuilder descriptor = new StringBuilder();

        for (int i = 0; i < types.length; i++) {
            descriptor.append(types[i])
                       .append('[')
                       .append(i)
                       .append("](")
                       .append(fields[i])
                       .append(')')
                       .append(i < types.length - 1 ? ", " : "");
        }

        return descriptor.toString();
    }
}
