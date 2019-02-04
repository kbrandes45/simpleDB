package simpledb;

import java.util.*;


/**
 * Instance of HPIterator will iterate over collection of tuples in a given HeapPage
 *
 * @see HeapPage
 *
 */

public class HPIterator implements Iterator<Tuple> {

	public HPIterator(HeapPage hp) {
		this.heap_page = hp;
		this.num_tuples = hp.getNonEmptyTuples();
		this.current_tup_index = 0;
		
	}
	
	
	public boolean hasNext() {
		if (this.current_tup_index < this.num_tuples) {
			return true;
		}
		else {
			return false;
		}
	}

	
	public Tuple next() {
		// Can't return tuples in empty slots --> skip them
		return heap_page.tuples[this.current_tup_index++];
	}
	
	
	public void remove() throws UnsupportedOperationException {
		throw new UnsupportedOperationException("Not allowed to remove"); 
	}
	
	private int num_tuples;
	private HeapPage heap_page;
	private int current_tup_index;
	
}
