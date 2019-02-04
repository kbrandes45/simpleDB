package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private TransactionId txn_id;
    private int table_id;
    private String table_alias;
    private DbFile table_hf;
    private DbFileIterator seqscan_iterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
    	this.txn_id = tid;
    	this.table_id = tableid;
    	this.table_alias = tableAlias;
    	this.table_hf = Database.getCatalog().getDatabaseFile(this.table_id);
    	this.seqscan_iterator = this.table_hf.iterator(tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
    	//my code here
    	return Database.getCatalog().getTableName(this.table_id);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return this.table_alias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
    	this.table_id = tableid;
    	this.table_alias = tableAlias;
    	this.table_hf = Database.getCatalog().getDatabaseFile(tableid);
    	this.seqscan_iterator = this.table_hf.iterator(this.txn_id);
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	if (this.seqscan_iterator != null) {
    		this.seqscan_iterator.open();
    	}
    	else {
    		throw new DbException("No iterator for SeqScan");
    	}   	
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	TupleDesc schema = this.table_hf.getTupleDesc();
    	int tup_count = schema.numFields();
    	Type[] typeAr = new Type[tup_count];
    	String[] fieldAr = new String[tup_count];
    	for (int i = 0; i < tup_count; i++) {
    		typeAr[i] = schema.getFieldType(i);
    		String appended = this.table_alias + '.' + schema.getFieldName(i);
    		fieldAr[i] = appended;
    	}
        return new TupleDesc(typeAr, fieldAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return this.seqscan_iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        return this.seqscan_iterator.next();
    }

    public void close() {
        // some code goes here
    	this.seqscan_iterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	this.seqscan_iterator.rewind();
    }
}
