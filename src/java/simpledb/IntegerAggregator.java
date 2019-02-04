package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int grp_by_field_num;
    private Type grp_by_field_type;
    private int agg_field_num;
    private Op operation;
    private HashMap<Field,Integer> aggregated;
    private HashMap<Field, Integer> counts_for_agg;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.grp_by_field_num = gbfield;
    	this.grp_by_field_type = gbfieldtype;
    	this.agg_field_num = afield;
    	this.operation = what;
    	this.aggregated = new HashMap<Field, Integer>();
    	this.counts_for_agg = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    
    private Integer initialize_value() throws NoSuchElementException {
    	if (this.operation == null) {
    		throw new NoSuchElementException("Operator is null");
    	}
    	if (this.operation == Op.AVG || this.operation == Op.SUM || this.operation == Op.COUNT) {
    		return 0;
    	}
    	else if (this.operation == Op.MAX) {
    		return Integer.MIN_VALUE;
    	}
    	else if (this.operation == Op.MIN) {
    		return Integer.MAX_VALUE;
    	}
    	return null; //should never reach this case
    }
    
    public void mergeTupleIntoGroup(Tuple tup) throws NoSuchElementException {
        // some code goes here
    	//check if group by fields match --> if yes then merge the agg field based on op
    	if (this.aggregated != null) {
    		Field tupfield = (this.grp_by_field_num == Aggregator.NO_GROUPING) ? null : tup.getField(this.grp_by_field_num);
    		if (!this.aggregated.containsKey(tupfield)) {
    			//initialize to base value to check if a field is null before adding it
    			this.aggregated.put(tupfield, initialize_value()); 
    			this.counts_for_agg.put(tupfield, 0);
    		}
    		Integer new_agg_value = ((IntField) tup.getField(this.agg_field_num)).getValue(); //assuming agg_field number is valid... 
    		try {
				this.apply_operator(new_agg_value, tupfield);
			} catch (DbException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} //assuming the value in the agg_field is valid/not null....		
    	}
    }
    
    private void apply_operator(Integer current_agg, Field current_f) throws NoSuchElementException, DbException {
    	
    	//System.out.print(current_agg.toString()+current_f.toString());
    	if (this.operation == Op.MAX) {
    		Integer agg = this.aggregated.get(current_f);
    		if (current_agg > agg) {
    			this.aggregated.put(current_f, current_agg);
    		} 
    	}
    	else if (this.operation == Op.MIN) {
    		Integer agg = this.aggregated.get(current_f);
    		if (current_agg < agg) {
    			this.aggregated.put(current_f, current_agg);
    		}
    	}
    	else if (this.operation == Op.COUNT) {
    		Integer current_count = this.counts_for_agg.get(current_f);
    		this.counts_for_agg.put(current_f, current_count+1); //duplicate data essentially... 
    		this.aggregated.put(current_f, current_count+1);
    	}
    	else if (this.operation == Op.SUM) {
    		//System.out.print(" "+current_agg.toString()+"current agg ");
    		Integer current_sum = this.aggregated.get(current_f);
    		Integer current_count = this.counts_for_agg.get(current_f);
    		this.counts_for_agg.put(current_f, current_count+1);
    		Integer new_sum = current_sum+current_agg;
    		this.aggregated.put(current_f, new_sum);
    		//System.out.print("field "+current_f.toString());
    		//System.out.print("sum: "+new_sum.toString());
    	}
    	else if (this.operation == Op.AVG) {
    		Integer current_count = this.counts_for_agg.get(current_f);
    		Integer current_sum = this.aggregated.get(current_f);
    		current_sum = current_sum + current_agg;
    		this.counts_for_agg.put(current_f, current_count+1);
    		//Integer new_avg = current_sum/(current_count + 1);
    		this.aggregated.put(current_f, current_sum);
    	}

    }

    private TupleDesc gen_tupdesc() {
    	String[] field_ar;
    	Type[] type_ar;
    	if (this.grp_by_field_num == Aggregator.NO_GROUPING) {
    		type_ar = new Type[] {Type.INT_TYPE};//{this.grp_by_field_type};
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
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> agg_tups = new ArrayList<Tuple>();
        TupleDesc td = this.gen_tupdesc();
        for (Field f : this.aggregated.keySet()) {
        	Tuple new_tup = new Tuple(td);
        	Integer val;
        	if (this.operation == Op.AVG){
        		val = this.aggregated.get(f)/this.counts_for_agg.get(f);
        		//System.out.print(val.toString()+"AVGERAGE HERE");
        	}
        	else {
        		val = this.aggregated.get(f);
        	}
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
