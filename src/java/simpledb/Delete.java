package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private OpIterator kid;
    private boolean already_called;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
    	this.tid = t;
    	this.kid = child;
    	this.already_called = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	Type[] type_ar = new Type[] {Type.INT_TYPE};//{this.grp_by_field_type};
		String[] field_ar = new String[] {"Deleted Tuples Count"};
    	TupleDesc td = new TupleDesc(type_ar, field_ar);
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	if (this.kid != null) {
    		this.kid.open();
    	}
    	else {
    		throw new DbException("OpIterator for delete is null");
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (this.already_called) { 
    		return null; 
    	}
    	int delete_count = 0;
    	while (this.kid.hasNext()) {
    		Tuple t = this.kid.next();
    		try {
				Database.getBufferPool().deleteTuple(this.tid, t);
			} catch (IOException e) {
				throw new DbException("Deleting tuple caused i/o exception");
			}
    		delete_count += 1;
    	}
    	TupleDesc td = this.getTupleDesc();
    	Tuple output_tup = new Tuple(td);
    	output_tup.setField(0, new IntField(delete_count));
    	this.already_called = true;
        return output_tup;
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
