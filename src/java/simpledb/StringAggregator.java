package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int grp_by_field_num;
    private Type grp_by_field_type;
    private int agg_field_num;
    private Op operation;
    private HashMap<Field,Integer> aggregated;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) 
    		throws IllegalArgumentException {
        // some code goes here
    	if (what != Op.COUNT) {
    		throw new IllegalArgumentException("this operator can't be for strings");
    	} //or should this be an assert to stop iteration?
    	this.grp_by_field_num = gbfield;
    	this.grp_by_field_type = gbfieldtype;
    	this.agg_field_num = afield;
    	this.operation = what;
    	this.aggregated = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	if (this.aggregated != null) {
    		Field tupfield = (this.grp_by_field_num == Aggregator.NO_GROUPING) ? null : tup.getField(this.grp_by_field_num);
    		if (!this.aggregated.containsKey(tupfield)) {
    			//initialize to base value to check if a field is null before adding it
    			this.aggregated.put(tupfield, 0); 
    		}
    		Integer current_count = this.aggregated.get(tupfield);
    		this.aggregated.put(tupfield, current_count+1);
    	}
    }
    
    private TupleDesc gen_tupdesc() {
    	String[] field_ar;
    	Type[] type_ar;
    	if (this.grp_by_field_num == Aggregator.NO_GROUPING) {
    		type_ar = new Type[] {Type.STRING_TYPE};//this.grp_by_field_type};
    		field_ar = new String[] {"no_grouping"};
    	}
    	else {
    		type_ar = new Type[] {this.grp_by_field_type, Type.INT_TYPE};
    		field_ar = new String[] {this.grp_by_field_type.toString(), Type.INT_TYPE.toString()};
    	}
    	return new TupleDesc(type_ar, field_ar);
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
    	ArrayList<Tuple> agg_tups = new ArrayList<Tuple>();
    	TupleDesc td = gen_tupdesc();
    	for (Field f : this.aggregated.keySet()) {
        	Tuple new_tup = new Tuple(td);
        	Integer val = this.aggregated.get(f);
        	if (this.grp_by_field_num == Aggregator.NO_GROUPING) {
        		new_tup.setField(0, new IntField(val));
        	} else {
        		new_tup.setField(0, f);
        		new_tup.setField(1, new IntField(val));	
        	}
        	agg_tups.add(new_tup);   	
        }	
    	return new TupleIterator(td, agg_tups);
    }

}
