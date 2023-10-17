// Sushant Ramesh and Anders Pesti
package hw1;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.ArrayList;
import java.util.Arrays;

public class HeapPage {
    private int id;
    private byte[] header;
    private Tuple[] tuples;
    private TupleDesc td;
    private int numSlots;
    private int tableId;

    public HeapPage(int id, byte[] data, int tableId) throws IOException {
    	if (data.length != HeapFile.PAGE_SIZE) {
    		throw new IllegalArgumentException("Data length does not match page size");
    	}
        this.id = id;
        this.tableId = tableId;

        this.td = Database.getCatalog().getTupleDesc(this.tableId);
        this.numSlots = getNumSlots();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++)
            header[i] = dis.readByte();

        try {
            tuples = new Tuple[numSlots];
            for (int i = 0; i < tuples.length; i++) {
            	if (dis.available() < td.getSize()) {
            		break;
            	}
            	tuples[i] = readNextTuple(dis, i);
            }
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();
    }

    public int getId() {
        return this.id;
    }

    public int getNumSlots() {
    	/*
    	int headerSize = getHeaderSize();
        return (HeapFile.PAGE_SIZE - headerSize) / this.td.getSize();
        */
    	return (int)Math.floor(HeapFile.PAGE_SIZE / (td.getSize()+(1/8.0)));
    }

    private int getHeaderSize() {
        return (int)Math.ceil(getNumSlots()/8.0);

    }

    public boolean slotOccupied(int s) {
        int byteIndex = s / 8;
        int bitIndex = s % 8;
        boolean isOccupied = (header[byteIndex] & (1 << bitIndex)) != 0;
        //System.out.println("Slot " + s + " occupied: " + isOccupied);
        return isOccupied;
    }

    public void setSlotOccupied(int s, boolean value) {
        int byteIndex = s / 8;
        int bitIndex = s % 8;
        if (value) {
            header[byteIndex] |= (1 << bitIndex);
        } else {
            header[byteIndex] &= ~(1 << bitIndex);
        }
    }

    public void addTuple(Tuple t) throws Exception {
        for (int i = 0; i < numSlots; i++) {
            if (!slotOccupied(i)) {
                tuples[i] = t;
                setSlotOccupied(i, true);
                return;
            }
        }
        throw new Exception("every spot is taken");
    }

    public void deleteTuple(Tuple t) throws Exception {
        int tupleSlot = t.getId();
        if (!slotOccupied(tupleSlot)) {
            throw new Exception("Slot is empty");
        }
        tuples[tupleSlot] = null;
        setSlotOccupied(tupleSlot, false);
    }
    
    private Tuple readNextTuple(DataInputStream dis, int slotId) {
    	
    	if (!slotOccupied(slotId)) {
    		int bytesToSkip = td.getSize();
    		try {
    			if (dis.available() < bytesToSkip) {
    				throw new NoSuchElementException("Reached end of stream");
    			}
    			for (int i = 0; i < bytesToSkip; i++) {
    				dis.readByte();
    			}
    			
    		} catch (IOException e) {
    			throw new NoSuchElementException("Error reading from Stream");
    		}
    		return null;
    	}

        Tuple t = new Tuple(td);
        t.setPid(this.id);
        t.setId(slotId);

        for (int j=0; j<td.numFields(); j++) {
            if(td.getType(j) == Type.INT) {
                //System.out.println("Reading INT field for slot: " + slotId + ", field index: " + j);
                byte[] field = new byte[4];
                try {
                	if (dis.available() < 4) {
                		throw new NoSuchElementException("Not enough space");
                	}
                    dis.read(field);
                    
                    
                    t.setField(j, new IntField(field));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                //System.out.println("Reading STRING field for slot: " + slotId + ", field index: " + j);
                byte[] field = new byte[129];
                try {
                    dis.read(field);
                    t.setField(j, new StringField(field));
                } catch (IOException e) {
                    //System.out.println("IO Exception while reading slot: " + slotId + ", field index: " + j);
                    e.printStackTrace();
                }
            }
        }
        return t;
    }
    
    public byte[] getPageData() {
        int len = HeapFile.PAGE_SIZE;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);
        
        for (int i=0; i<header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        for (int i=0; i<tuples.length; i++) {
            if (!slotOccupied(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                continue;
            }

            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    dos.write(f.toByteArray());
                    
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        int zerolen = HeapFile.PAGE_SIZE - (header.length + td.getSize() * tuples.length);
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    public Iterator<Tuple> iterator() {
        ArrayList<Tuple> occupiedTuples = new ArrayList<>();
        for (int i = 0; i < tuples.length; i++) {
            if (slotOccupied(i)) {
                occupiedTuples.add(tuples[i]);
            }
        }
        return occupiedTuples.iterator();
    }
}
