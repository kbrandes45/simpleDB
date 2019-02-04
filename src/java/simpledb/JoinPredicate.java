package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;
    private int field1_index;
    private int field2_index; 
    private Predicate.Op operation;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        // some code goes here
    	this.operation = op;
    	this.field1_index = field1;
    	this.field2_index = field2;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here
    	Field t1_f = t1.getField(field1_index);
    	Field t2_f = t2.getField(field2_index);
        return t1_f.compare(this.operation, t2_f);
    }
    
    public int getField1()
    {
        // some code goes here
        return this.field1_index;
    }
    
    public int getField2()
    {
        // some code goes here
        return this.field2_index;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here
        return this.operation;
    }
}
