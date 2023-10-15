//Sushant Ramesh
package hw1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;


public class Catalog {
	private class Table{
		public final String tableName;
		public final HeapFile file;
		public final String pkeyField;
		
		public Table(String name, HeapFile file, String keyField) {
			this.tableName = name;
			this.file = file;
			this.pkeyField = keyField;
		}
	}
  
	private List<Table> tables;
	private Map<String, Integer> nameToId;
    public Catalog() {
    	this.tables = new ArrayList<>();
    	this.nameToId = new HashMap<>();
    }

 
    public void addTable(HeapFile file, String name, String pkeyField) {
    	if (name == null) {
    		throw new IllegalArgumentException();
    	}
    	int id = file.getId();
    	
    	if(nameToId.containsKey(name)) {
    		int oldTableIndex = nameToId.get(name);
    		tables.set(oldTableIndex, new Table(name, file, pkeyField));
    	} else {
    		tables.add(new Table(name, file, pkeyField));
    		nameToId.put(name,  tables.size()-1);
    	}
    }

    public void addTable(HeapFile file, String name) {
        addTable(file,name,"");
    }

    
    public int getTableId(String name) throws NoSuchElementException {
        if (!nameToId.containsKey(name)) {
            throw new NoSuchElementException();
        }
        int tableIndex = nameToId.get(name);
        return tables.get(tableIndex).file.getId(); 
    }

 
   public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
       for (Table table : tables) {
           if (table.file.getId() == tableid) {
               return table.file.getTupleDesc();
           }
       }
       throw new NoSuchElementException();
   }

 
   public HeapFile getDbFile(int tableid) throws NoSuchElementException {
       // Search through the tables ArrayList to find the table with the specified id
       for (Table table : tables) {
           if (table.file.getId() == tableid) {
               return table.file;
           }
       }
       
       throw new NoSuchElementException();
   }

   
   public void clear() {
       nameToId.clear();
       tables.clear();  
   }


   	public String getPrimaryKey(int tableid) {
       
       for (Table table : tables) {
           if (table.file.getId() == tableid) {
               return table.pkeyField;
           }
       }

       throw new NoSuchElementException();
   }

    
    public Iterator<Integer> tableIdIterator() {
        // Create a list to hold the table ids
        List<Integer> tableIds = new ArrayList<>();
        for (Table table : tables) {
            tableIds.add(table.file.getId());
        }
        return tableIds.iterator();
    }

    public String getTableName(int id) {
        
        for (Table table : tables) {
            if (table.file.getId() == id) {
                return table.tableName;
            }
        }
       
        throw new NoSuchElementException();
    }
   
    
    public void loadSchema(String catalogFile) {
        String line = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File("testfiles/" + name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}