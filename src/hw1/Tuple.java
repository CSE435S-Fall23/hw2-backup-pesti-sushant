// Sushant Ramesh and Anders Pesti
package hw1;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;


public class Tuple {
	private TupleDesc tupledesc;
	private int pid;
	private int id;
	private ArrayList<Field> fields;
	

	public Tuple(TupleDesc tuple) {
		this.tupledesc = tuple;
		this.fields = new ArrayList<>();
	}
	
	public TupleDesc getDesc() {
		return this.tupledesc;
	}
	

	public int getPid() {
		return this.pid;
	}

	public void setPid(int pid) {
		this.pid = pid;
	}


	public int getId() {
		return this.id;
	}

	public void setId(int id) {
		this.id = id;
	}
	
	public void setDesc(TupleDesc td) {
		this.tupledesc = td;
	}
	
	
	public void addField(Field field) {
		fields.add(field);
		}
	public void setField(int i, Field v) {
		while(fields.size()<=i) {
			fields.add(null);
		}
		fields.set(i, v);
	}
	
	public Field getField(int index) {
		return fields.get(index);
	}
	
	
	public String toString() {
		StringBuilder result = new StringBuilder();
	    for (int i = 0; i < this.fields.size(); i++) {
	        String fieldName = this.tupledesc.getFieldName(i);
	        Field fieldVal = this.fields.get(i);  
	        result.append(fieldName)
	               .append(": ")
	               .append(fieldVal.toString())
	               .append(i < this.fields.size() - 1 ? ", " : "");  
	    }
	    return result.toString();
	}
}
