package simpledb.storage;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private TupleDesc tupleDesc;

    private final List<Field> tupleFields;

    private RecordId recordId;

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.tupleDesc = td;
        this.tupleFields = new ArrayList<>(td.numFields());
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if(i >= this.tupleDesc.numFields() || i < 0) throw new ArrayIndexOutOfBoundsException();
        if(i >= this.tupleFields.size()) this.tupleFields.add(i, f);
        else this.tupleFields.set(i, f);
//        this.tupleFields.toArray()[i] = f;
//        System.out.println(this.tupleFields);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * @param i
     *             index of the field to get. It must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        if(i >= this.tupleDesc.numFields() || i < 0) return null;
        else return this.tupleFields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\t column2\t column3\t...\t columnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        return "TupleFields: " + this.tupleFields;
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return this.tupleFields.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        this.tupleDesc = td;
        // need to change tupleFields ?
    }
}
