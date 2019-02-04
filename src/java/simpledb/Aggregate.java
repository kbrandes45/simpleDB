package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator kid;
    private Aggregator.Op operation;
    private int grp_by_field_num;
    private int agg_field_num;
    private Aggregator do_agg;
    private OpIterator agg_iterator;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
    	// some code goes here
    	assert(child!=null);
    	this.kid = child;
    	this.agg_field_num = afield;
    	this.grp_by_field_num = gfield;
    	this.operation = aop; //hoping this is checked as valid within the aggregators
    	
    	TupleDesc td = child.getTupleDesc();
    	Type agg_field_type = td.getFieldType(afield);
    	assert(agg_field_type != null);
    	Type grp_field_type = null;
    	if (!(gfield == Aggregator.NO_GROUPING)) {
    		grp_field_type = td.getFieldType(gfield);
    	}
    	if (agg_field_type == Type.INT_TYPE) {
    		this.do_agg = new IntegerAggregator(gfield, grp_field_type, afield, aop);
    	}
    	if (agg_field_type == Type.STRING_TYPE) {
    		this.do_agg = new StringAggregator(gfield, grp_field_type, afield, aop);
    	}
    	
    	this.agg_iterator = null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
    	// some code goes here
    	return this.grp_by_field_num;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
    	// some code goes here
    	if (this.grp_by_field_num == Aggregator.NO_GROUPING) {
    		return null;
    	}
    	else {
    		if (this.agg_iterator != null) {
    			TupleDesc td = this.agg_iterator.getTupleDesc();
    			return td.getFieldName(this.grp_by_field_num);
    		}
    		return null;
    	}
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
    	// some code goes here
    	return this.agg_field_num;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
    	// some code goes here
    	if (this.agg_iterator != null) {
    		TupleDesc td = this.agg_iterator.getTupleDesc();
    		return td.getFieldName(this.agg_field_num);
    	}
    	return null;
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
    	// some code goes here
    	return this.operation;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
    	// some code goes here
    	super.open();
    	this.kid.open(); //exists because we asserted not null in constructor
    	while (this.kid.hasNext()) { //Aggregating all the tuples
    		this.do_agg.mergeTupleIntoGroup(this.kid.next());
    	}
    	
    	this.agg_iterator = this.do_agg.iterator();
    	this.agg_iterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	// some code goes here
    	while (this.agg_iterator.hasNext()) {
    		return this.agg_iterator.next();	
    	}
    	return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	// some code goes here
    	this.agg_iterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
    	// some code goes here
    	return this.kid.getTupleDesc();
    }

    public void close() {
    	// some code goes here
    	super.close();
    	this.kid.close();
    	this.agg_iterator.close();
    }

    @Override
    public OpIterator[] getChildren() {
    	// some code goes here
    	return new OpIterator[] {kid};
    }

    @Override
    public void setChildren(OpIterator[] children) {
    	// some code goes here
    	kid = children[0];
    }
    
}
