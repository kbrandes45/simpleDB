package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
	private int num_buckets;
	private int max_value;
	private int min_value;
	private int range;
	private int[] bucket_storage;
	private int total_tups;
	
	
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	this.num_buckets = buckets;
    	this.bucket_storage = new int[buckets]; //buckets track number of tuples in a given range
    	this.max_value = max;
    	this.min_value = min;
    	this.range = (int) Math.ceil((double) (max-min+1)/this.num_buckets);
    	this.total_tups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	this.total_tups++;
    	//System.out.println(this.range+"RANGE");
    	int current_bucket = (v-this.min_value)/(this.range);
    	if (current_bucket > this.num_buckets-1)
    		current_bucket = this.num_buckets-1;
    	
    	//System.out.println("value: "+v+" currentb: "+current_bucket);
    	//System.out.println(v+" in bucket "+current_bucket);
    	this.bucket_storage[current_bucket]++;    	
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	switch(op) {
    		case EQUALS:
    			return equals_selectivity(v);
    		case LESS_THAN:
    			return lessthan_selectivity(v);
    		case LESS_THAN_OR_EQ:
    			return lessthanequals_selectivity(v);
    		case GREATER_THAN:
    			return greaterthan_selectivity(v);
    		case GREATER_THAN_OR_EQ:
    			return greaterthanequals_selectivity(v);
    		case NOT_EQUALS:
    			return 1.0 - equals_selectivity(v);
    		case LIKE:
    			return equals_selectivity(v);	
    		default:
    			return -1.0;
    	}

    }
    
    private double equals_selectivity(int v) {
    	//System.out.println(this.range+" "+this.num_buckets+" "+ this.min_value+" "+ this.max_value);
    	int current_bucket = (int) ((v-this.min_value)/(this.range));
    	//System.out.println(current_bucket);
    	if (current_bucket<0 ||current_bucket >= this.num_buckets) {
    		return 0.0;
    	}
    	
    	//Number of tuples at the value for the bucket = bucket_num_tuples/(number of steps in the range)
    	//System.out.println((double) this.bucket_storage[current_bucket]/this.range + " "+this.total_tups);
    	
    	double height = (double) this.bucket_storage[current_bucket]/this.range;
    	if (height == 0.0)
    		return 0.0;
    	double sf = ((double) height / (this.total_tups));
    	return sf;    	
    }
    
    private double lessthan_selectivity(int v) {
    	int current_bucket = (int) ((v-this.min_value)/(this.range));
    	if (current_bucket<0)  			
       		return 0.0;
    	else if (current_bucket > this.num_buckets) 
    		return 1.0;
    	//want number of tuples below the range value in the given bucket and all other buckets
    	int other_buckets = 0;
    	for (int i = 0; i<current_bucket; i++) {
    		other_buckets += this.bucket_storage[i];
    	}
    	int tups_per_step = (int) (this.bucket_storage[current_bucket]/this.range);
    	int num_steps = v - (this.min_value + current_bucket*this.range);
    	int current_b = num_steps*tups_per_step;
    	double sf = ((double) other_buckets+current_b)/this.total_tups;

    	return sf;
    }
    
    private double lessthanequals_selectivity(int v) {
    	int current_bucket = (int) ((v-this.min_value)/(this.range));
    	if (current_bucket<0)  			
       		return 0.0;
    	else if (current_bucket > this.num_buckets) 
    		return 1.0;
    	//want number of tuples below the range value in the given bucket and all other buckets
    	int other_buckets = 0;
    	for (int i = 0; i<current_bucket; i++) {
    		other_buckets += this.bucket_storage[i];
    	}
    	int tups_per_step = (int) (this.bucket_storage[current_bucket]/this.range);
    	int num_steps = v - (this.min_value + current_bucket*this.range)+1;//+1 to include the value v's tuples
    	int current_b = num_steps*tups_per_step;
    	double sf = ((double) other_buckets+current_b)/this.total_tups;
    	return sf;
    }
    
    private double greaterthan_selectivity(int v) {
    	int current_bucket = (int) ((v-this.min_value)/(this.range));
    	if (current_bucket > this.num_buckets) 
    		return 0.0;
    	else if (current_bucket<0)
    		return 1.0;
    	//want number of tuples below the range value in the given bucket and all other buckets
    	int other_buckets = 0;
    	for (int i = current_bucket+1; i<this.num_buckets; i++) {
    		other_buckets += this.bucket_storage[i];
    	}
    	int tups_per_step = (int) (this.bucket_storage[current_bucket]/this.range);
    	int num_steps = (this.min_value + (current_bucket+1)*this.range)-v;
    	int current_b = num_steps*tups_per_step;
    	double sf = ((double) other_buckets+current_b)/this.total_tups;
    	return sf;
    }
    
    private double greaterthanequals_selectivity(int v) {
    	int current_bucket = (int) ((v-this.min_value)/(this.range));
    	if (current_bucket > this.num_buckets) 
    		return 0.0;
    	else if (current_bucket<0)
    		return 1.0;
    	//want number of tuples below the range value in the given bucket and all other buckets
    	int other_buckets = 0;
    	for (int i = current_bucket+1; i<this.num_buckets; i++) {
    		other_buckets += this.bucket_storage[i];
    	}
    	int tups_per_step = (int) (this.bucket_storage[current_bucket]/this.range);
    	//System.out.println(tups_per_step);
    	int num_steps = (this.min_value + (current_bucket+1)*this.range)-v; 
    	//System.out.println(num_steps);
    	int current_b = num_steps*tups_per_step;
    	double sf = ((double) other_buckets+current_b)/this.total_tups;
    	//System.out.println(sf+" for value "+v);
    	return sf;
    }
    
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
    	String s = "Hist: "+this.num_buckets+" max "+this.max_value+" min "+this.min_value+" total tups "+this.total_tups+" end";
        return s;
    }
}
