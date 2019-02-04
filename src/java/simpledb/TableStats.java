package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.TupleDesc.TDItem;


/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    private HashMap<Integer, StringHistogram> string_hists;
    private HashMap<Integer, IntHistogram> int_hists;
    private int num_tuples;
    private TupleDesc fields;
    private int io_cost_per_page;
    private DbFile table;
    private int num_hist_buckets = 100;
    private int tups_per_page;
    private int num_pages;
    
    
    
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    		
    	this.string_hists = new HashMap<Integer, StringHistogram>();
    	this.int_hists = new HashMap<Integer, IntHistogram>();
    	this.num_tuples = 0;
    	
    	this.table = Database.getCatalog().getDatabaseFile(tableid);
    	this.fields = this.table.getTupleDesc();
    	this.num_pages = ((HeapFile) this.table).numPages();
    	this.tups_per_page = Database.getBufferPool().getPageSize();		
    	this.io_cost_per_page = ioCostPerPage;
    	
    	
    	this.set_histograms();
    	System.out.println("HISTS ARE DONE");
    	   	
    }
    
    private void set_histograms() {
    	int[] mins = new int[this.fields.numFields()];
    	int[] maxs = new int[this.fields.numFields()];
    	
    	TransactionId tid = new TransactionId();
    	DbFileIterator table_iter = this.table.iterator(tid);
    	try {
		table_iter.open();
    	while (table_iter.hasNext()) {
			Tuple t = table_iter.next();
			Iterator<Field> it = t.fields();
			for (int i = 0; i < this.fields.numFields(); i++) {
				Field f = it.next();
				if (f.getType() == Type.INT_TYPE) {
					int value = ((IntField) f).getValue();
					if (value > maxs[i]) {
						maxs[i] = value;
					}

					if (value < mins[i]) {
						mins[i] = value;
					}
				}
			}

			this.num_tuples++;
    	}
    	
    	Iterator<TDItem> tup_desc_it = this.fields.iterator();
    	for (int i = 0; i < this.fields.numFields(); i++) {
    		if (tup_desc_it.next().fieldType == Type.INT_TYPE) {
    			// Use at most NUM_HIST_BINS.
    			this.int_hists.put(i,
    					 new IntHistogram( Math.min(this.num_hist_buckets, maxs[i] - mins[i] + 1),
    							 			mins[i],maxs[i]));
    		} else {
    			this.string_hists.put(i, new StringHistogram(this.num_hist_buckets));
    		}
    	}
    	
    	table_iter.rewind();
    	
    	while (table_iter.hasNext()) {
			Tuple t = table_iter.next();
			Iterator<Field> it = t.fields();
			for (int i = 0; i < this.fields.numFields(); i++) {
				Field f = it.next();
				if (f.getType() == Type.INT_TYPE) {
					this.int_hists.get(i).addValue(((IntField) f).getValue());
				} else {
					this.string_hists.get(i).addValue(((StringField) f).getValue());
				}
			}
    	}
    	table_iter.close();
    	} catch (DbException | TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    	
    }

    
    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return this.num_pages*this.io_cost_per_page;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
    	int out = (int) Math.ceil(selectivityFactor*this.num_tuples);
        return out;
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
    	double sf = 1.0;
    	//System.out.println(this.num_tuples);
    	if (this.fields.getFieldType(field) == Type.INT_TYPE) {
    		IntHistogram ih = this.int_hists.get(field);
    		//System.out.print(ih.toString());
    		sf = ih.estimateSelectivity(op, ((IntField) constant).getValue());
    	}
    	if (this.fields.getFieldType(field) == Type.STRING_TYPE) {
    		StringHistogram sh = this.string_hists.get(field);
    		sf = sh.estimateSelectivity(op, ((StringField) constant).getValue());
    	}
        return sf;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.num_tuples;
    }

}
