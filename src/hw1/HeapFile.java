// Sushant Ramesh and Anders Pesti
package hw1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFile {
	
	public static final int PAGE_SIZE = 4096;
	private File file;
	private TupleDesc td;
	
	public HeapFile(File f, TupleDesc type) {
		this.file = f;
		this.td = type;
	}
	
	public File getFile() {
		return this.file;
	}
	
	public TupleDesc getTupleDesc() {
		return this.td;
	}
	
	public HeapPage readPage(int id) {
		try {
			RandomAccessFile raf = new RandomAccessFile(this.file, "r");
			byte[] data = new byte[PAGE_SIZE];
			raf.seek(id * PAGE_SIZE);
			raf.read(data, 0, PAGE_SIZE);
			raf.close();
			return new HeapPage(id, data, this.getId());
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public int getId() {
		return this.file.getAbsoluteFile().hashCode();
	}
	
	public void writePage(HeapPage p) {
		try {
			RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
			byte[] data = p.getPageData();
			raf.seek(p.getId() * PAGE_SIZE);
			raf.write(data);
			raf.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public HeapPage addTuple(Tuple t) {
		for (int i = 0; i < getNumPages(); i++) {
			HeapPage page = readPage(i);
			try {
				page.addTuple(t);
				writePage(page);
				return page;
			} catch (Exception e) {
			}
		}
		try {
			HeapPage newPage = new HeapPage(getNumPages(), new byte[PAGE_SIZE], getId());
			newPage.addTuple(t);
			writePage(newPage);
			return newPage;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public void deleteTuple(Tuple t) throws Exception {
	    int pageId = t.getPid();
	    
	    HeapPage page = readPage(pageId);
	    
	    page.deleteTuple(t);
	    
	    writePage(page);
	}


	public ArrayList<Tuple> getAllTuples() {
		ArrayList<Tuple> allTuples = new ArrayList<>();
		for (int i = 0; i < getNumPages(); i++) {
			HeapPage page = readPage(i);
			Iterator<Tuple> pageIterator = page.iterator();
			while (pageIterator.hasNext()) {
				allTuples.add(pageIterator.next());
			}
		}
		return allTuples;
	}
	
	public int getNumPages() {
		return (int) (this.file.length() / PAGE_SIZE);
	}
}
