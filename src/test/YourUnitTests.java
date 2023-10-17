package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.junit.Before;
import org.junit.Test;

import hw1.Catalog;
import hw1.Database;
import hw1.IntField;
import hw1.Query;
import hw1.Relation;
import hw1.Tuple;

import org.junit.Test;

public class YourUnitTests {
	
	private Catalog c;
	
	@Before
    public void setup() {
        try {
            Files.copy(new File("testfiles/A.dat.bak").toPath(), new File("testfiles/A.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            System.out.println("unable to copy files");
            e.printStackTrace();
        }
        
        c = Database.getCatalog();
        c.loadSchema("testfiles/A.txt");
    }

	@Test
	public void testAsCase() {
		Query q = new Query("SELECT a1 AS bladee, a2 AS drain FROM A");
	    Relation r = q.execute();
	    
	    assertTrue(r.getDesc().getFieldName(0).equals("bladee"));
	    assertTrue(r.getDesc().getFieldName(1).equals("drain"));
	    
	    assertTrue(r.getTuples().size() == 8);
	    assertTrue(r.getDesc().getSize() == 8);
	}

}
