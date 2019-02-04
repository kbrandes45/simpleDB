package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private OpIterator kid;
    private int table_id;
    private boolean already_called;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
    	this.tid = t;
    	HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
    	if (!child.getTupleDesc().equals(hf.getTupleDesc())) {
    		throw new DbException("Not matching tuple descents");
    	}
    	this.table_id = tableId;
    	this.kid = child;
    	this.already_called = false;
    }

    public TupleDesc getTupleDesc() {
    	//this wants tuple desc of the OUTPUT
        // some code goes here
    	Type[] type_ar = new Type[] {Type.INT_TYPE};//{this.grp_by_field_type};
		String[] field_ar = new String[] {"Inserted Tuples Count"};
    	TupleDesc td = new TupleDesc(type_ar, field_ar);
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	if (this.kid != null) {
    		this.kid.open();
    		//this.already_called = false;
    	}
    	else {
    		throw new DbException("Null opiterator");
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
    	this.already_called = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	//DO WE CHECK IF DUPLICATE SOMEWHERE ELSE?
    	if (this.already_called) { 
    		return null; 
    	}
    	int insert_count = 0;
    	while (this.kid.hasNext()) {
    		Tuple t = this.kid.next();
    		try {
				Database.getBufferPool().insertTuple(this.tid, this.table_id, t);
			} catch (IOException e) {
				throw new DbException("Inserting tuple caused i/o exception");
			}
    		insert_count += 1;
    	}
    	TupleDesc td = this.getTupleDesc();
    	Tuple output_tup = new Tuple(td);
    	output_tup.setField(0, new IntField(insert_count));
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
