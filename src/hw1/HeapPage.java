//Sushant Ramesh
package hw1;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.ArrayList;

public class HeapPage {
    private int id;
    private byte[] header;
    private Tuple[] tuples;
    private TupleDesc td;
    private int numSlots;
    private int tableId;

    public HeapPage(int id, byte[] data, int tableId) throws IOException {
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
            for (int i = 0; i < tuples.length; i++)
                tuples[i] = readNextTuple(dis, i);
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();
    }

    public int getId() {
        return this.id;
    }

    public int getNumSlots() {
        return (HeapFile.PAGE_SIZE - getHeaderSize()) / this.td.getSize();
    }

    private int getHeaderSize() {
        return (int) Math.ceil((double) this.numSlots / 8);
    }

    public boolean slotOccupied(int s) {
        int byteIndex = s / 8;
        int bitIndex = s % 8;
        return (header[byteIndex] & (1 << bitIndex)) != 0;
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
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("Got an error from reading empty tuple");
                }
            }
            return null;
        }

        Tuple t = new Tuple(td);
        t.setPid(this.id);
        t.setId(slotId);

        for (int j=0; j<td.numFields(); j++) {
            if(td.getType(j) == Type.INT) {
                byte[] field = new byte[4];
                try {
                    dis.read(field);
                    t.setField(j, new IntField(field));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                byte[] field = new byte[129];
                try {
                    dis.read(field);
                    t.setField(j, new StringField(field));
                } catch (IOException e) {
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
