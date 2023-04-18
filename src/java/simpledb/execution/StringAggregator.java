package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.io.Serial;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private final int gbfield;

    private final Type gbfieldtype;

    private final int afield;

    private final Op what;

    private TupleDesc td;

    private final HashMap<Field, Integer> GroupMap;

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(what != Op.COUNT) throw new IllegalArgumentException("Only Count AggregateOp For StringField!");
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.GroupMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(td == null) td = tup.getTupleDesc();
        Field gbField;
        if(this.gbfield == NO_GROUPING) {
            gbField = new IntField(0);
        }
        else {
            gbField = tup.getField(this.gbfield);
            assert gbField.getType() == this.gbfieldtype;
        }
        int x = GroupMap.getOrDefault(gbField, 0);
        GroupMap.put(gbField, x + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        TupleDesc td;
        ArrayList<Tuple> al = new ArrayList<>();
        if(this.gbfield == NO_GROUPING) {
            TupleDesc.TDItem td2 = new TupleDesc.TDItem(Type.INT_TYPE, this.td.getFieldName(afield));
            td = new TupleDesc(Collections.singletonList(td2));
            for(Field key : GroupMap.keySet()) {
                Tuple t = new Tuple(td);
                t.setField(0, new IntField(GroupMap.get(key)));
                al.add(t);
            }
        }
        else {
            TupleDesc.TDItem td1 = new TupleDesc.TDItem(gbfieldtype, this.td.getFieldName(gbfield));
            TupleDesc.TDItem td2 = new TupleDesc.TDItem(Type.INT_TYPE, this.td.getFieldName(afield));
            td = new TupleDesc(Arrays.asList(td1, td2));
            for(Field key : GroupMap.keySet()) {
                Tuple t = new Tuple(td);
                t.setField(0, key);
                t.setField(1, new IntField(GroupMap.get(key)));
                al.add(t);
            }
        }
        return new TupleIterator(td, al);
    }

}
