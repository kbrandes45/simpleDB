package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate pred;
    private OpIterator kid;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
    	this.pred = p;
    	this.kid = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.pred;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	return this.kid.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	// some code goes here
    	if (this.kid != null) {
    		super.open();
    		this.kid.open();
    	}
    	else {
    		throw new NoSuchElementException("no kid iterator");
    	}
    }

    public void close() {
        // some code goes here
    	super.close();
    	this.kid.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	this.kid.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
    	if (this.kid == null) {
    		throw new NoSuchElementException("no kid iterator");
    	}
    	else {
    		while(this.kid.hasNext()) {
    			Tuple current = this.kid.next();
    			if (this.pred.filter(current)) {
    				return current;
    			}
    			
    		}
    	}
        return null;
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
