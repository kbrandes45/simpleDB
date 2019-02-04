package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

	public Vector<TDItem> tup_desc;
	
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
    	//your code goes here
        return tup_desc.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	//your code goes here
    	tup_desc = new Vector<TDItem>(typeAr.length);
        for (int i =0; i<typeAr.length; i++) {
        	tup_desc.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	tup_desc = new Vector<TDItem>(typeAr.length);
    	for (int i = 0; i < typeAr.length; i++) {
    		tup_desc.add(new TDItem(typeAr[i],null));
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here

        return tup_desc.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
    	if (i<0 || i > numFields()) {
    		throw new NoSuchElementException();
    	} else {
    		return tup_desc.get(i).fieldName;
    	}
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
    	if (i<0 || i > numFields()) {
    		throw new NoSuchElementException();
    	} else {
    		return tup_desc.get(i).fieldType;
    	}
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
    	if (name == null) {
    		throw new NoSuchElementException();
    	}
    	
    	for (int i = 0; i < tup_desc.size(); i++) {
    		TDItem td = tup_desc.get(i);
    		if (td.fieldName == null) 
    			continue;
    		if (td.fieldName.equals(name)) {
    			return i;
    		}
    	}
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	int tuple_size = 0;
    	for (TDItem td : tup_desc) {
    		tuple_size += td.fieldType.getLen();
    	}
        return tuple_size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
    	Type[] typeAr = new Type[td1.numFields()+td2.numFields()];
    	String[] fieldAr = new String[td1.numFields()+td2.numFields()];
    	for (int i = 0; i<td1.numFields(); i++) {
    		typeAr[i] = td1.getFieldType(i);
    		fieldAr[i] = td1.getFieldName(i);
    	}
    	for (int j = td1.numFields(); j<(td2.numFields()+td1.numFields()); j++) {
    		typeAr[j] = td2.getFieldType(j-td1.numFields());
    		fieldAr[j] = td2.getFieldName(j-td1.numFields());
    	}
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
    	//can object ever not be a tuple desc
    	if (o ==null)
    		return false;
    	if (!(o instanceof TupleDesc)){
    		return false;
    	} else {
    		TupleDesc other_tup = (TupleDesc) o;
    		if (other_tup.numFields() != this.numFields()) {
    			return false;
    		} else {
    			for (int i = 0; i<this.numFields(); i++) {
    				if (!(this.getFieldType(i).equals(other_tup.getFieldType(i)))) {
    					return false;
    				}
    			}
    			return true;
    		}
    	}
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
    	StringBuffer descriptor = new StringBuffer();
    	for (int i = 0; i< this.numFields(); i++) {
    		descriptor.append(tup_desc.get(i).toString());
    		if (i < this.numFields()-1) {
    			descriptor.append(", ");
    		}
    	}
        return descriptor.toString();
    }
}
