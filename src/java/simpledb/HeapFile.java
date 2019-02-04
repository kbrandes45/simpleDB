package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	private TupleDesc tup_schema;
	private File heap_file;
	private int unique_id;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.heap_file = f;
    	this.tup_schema = td;
    	this.unique_id = f.getAbsoluteFile().hashCode();
    }
    

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.heap_file;
    }

    
    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.unique_id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tup_schema;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws UnsupportedOperationException {
        // some code goes here
    	int tableid = pid.getTableId();
    	final int pg_size = Database.getBufferPool().getPageSize();
    	byte[] output_page = HeapPage.createEmptyPageData();
    	try {
			RandomAccessFile rfile = new RandomAccessFile(this.heap_file, "r");
			rfile.seek(pg_size*pid.getPageNumber());
			rfile.readFully(output_page);
			rfile.close();
			return new HeapPage(new HeapPageId(tableid, pid.getPageNumber()), output_page);
    	
    	} catch (FileNotFoundException e) {
			throw new UnsupportedOperationException("file not found");
		} catch (IOException e) {
			throw new UnsupportedOperationException("io exception");
		}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1	
    	RandomAccessFile rfile = new RandomAccessFile(this.heap_file, "rw");
    	PageId pid = page.getId();
    	int page_size = Database.getBufferPool().getPageSize();
    	int start_pos = page_size * pid.getPageNumber();
    	rfile.seek(start_pos);
    	rfile.write(page.getPageData(), 0, page_size);
    	rfile.close();	
    	//System.out.print("new page"+ this.numPages());
    	//HeapPage hp = (HeapPage) page;
    	//System.out.print("empty slots"+hp.getNumEmptySlots());
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
    	final int page_size = Database.getBufferPool().getPageSize();
    	int hf_size = (int) this.heap_file.length();
        return (int) hf_size/page_size;
    }

    private HeapPage next_empty_page(TransactionId tid) throws TransactionAbortedException, DbException {
    	for (int i = 0; i < this.numPages(); i++) {
    		HeapPageId pid = new HeapPageId(this.getId(), i);
    		HeapPage new_page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
    		//System.out.print("check here");
    		if (new_page.getNumEmptySlots() > 0) {
    			//System.out.print("new page printed");
    			return new_page;
    		}
    	}
    	//System.out.print("null page");
    	return null;
    }
    
    
    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here - added in lab 2
    	HeapPage current_page = this.next_empty_page(tid);
    	ArrayList<Page> page_list = new ArrayList<Page>();
    	//System.out.print(current_page.toString());
    	if (current_page == null) {
    		//System.out.print("here");
    		//Create a new heap page
    		int new_page_no = this.numPages();
    		HeapPageId pid = new HeapPageId(this.getId(), new_page_no);
    		current_page = new HeapPage(pid, HeapPage.createEmptyPageData());
    		//Add page to disk
    		this.writePage(current_page);
    	}

    	PageId pid = current_page.pid;
    	HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    	hp.insertTuple(t); //could fail to insert if td doesnt match
    	page_list.add(hp);
    	
    	return page_list;   
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here - implemented in lab2
    	PageId pg = t.getRecordId().getPageId();
		HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pg, Permissions.READ_WRITE);
		page.deleteTuple(t);
		return new ArrayList<Page> (Arrays.asList(page));
    }
    
    private class HFIterator implements DbFileIterator {
    	
    	//constructor
    	public HFIterator(HeapFile hp, TransactionId tid) {
    		this.txn_id = tid;
    		this.tup_iterator = null;
    		this.current_page = null;
    		this.heap_file = hp;
    	}
    	
    	
		@Override
		public void open() throws DbException, TransactionAbortedException {	
			this.current_page = 0;
			this.tup_iterator = get_tup_iterator(this.current_page);
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			if (this.current_page != null) {
				while (this.current_page < this.heap_file.numPages() -1) {
					if (this.tup_iterator.hasNext()) {
						return true;
					}
					else {
						this.current_page++;
						this.tup_iterator = get_tup_iterator(this.current_page);
					}
				}
				return tup_iterator.hasNext();
			} 
			else {
				return false;
			}
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			if (hasNext()) {
				return tup_iterator.next();
			}
			else {
				throw new NoSuchElementException("No more tuples");
			}
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			close();
			open();
			
		}

		@Override
		public void close() {
			this.tup_iterator = null;
			this.current_page = null;
		}
		
		private Iterator<Tuple> get_tup_iterator(int pg) throws TransactionAbortedException, DbException {
			HeapPageId hp_pg_id = new HeapPageId(this.heap_file.getId(), this.current_page);
			HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.txn_id, hp_pg_id, Permissions.READ_ONLY);
			return page.iterator();			
		}
    	
		private Iterator<Tuple> tup_iterator;
    	private Integer current_page;
    	private final TransactionId txn_id;
    	private HeapFile heap_file;

    }
    

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HFIterator(this, tid);
    }

}

