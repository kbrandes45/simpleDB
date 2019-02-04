package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private int max_num_pages;
    private HashMap<PageId, Page> buffer_pages;
    private PageLocker lock_manager;
    private static int timeout = 200;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
    	max_num_pages = numPages;
    	buffer_pages = new HashMap<PageId, Page>();
    	lock_manager = new PageLocker();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
    	long start_time = System.currentTimeMillis();
    	boolean obtained_lock = this.lock_manager.acquire_lock(tid, pid, perm);
    	while (!obtained_lock) {
    		//blocking
    		long current_time = System.currentTimeMillis();
    		if (current_time - start_time > this.timeout) {
    			//time out, throw error bc not getting the lock due to deadlock
    			try {
					transactionComplete(tid, false);
				} catch (IOException e) {
					System.out.print("IO exception when aborting txn");
				}
    			throw new TransactionAbortedException();
    		}
    		try {
    			int sleeper = (int) Math.random()*10;
				Thread.sleep(sleeper+15);
			} catch (InterruptedException e) {
				System.out.print("issues with thread sleeping");
			}
    		obtained_lock = this.lock_manager.acquire_lock(tid, pid, perm);
	
    	}
    	if (!obtained_lock) //didnt get lock so cant have the page
    		throw new TransactionAbortedException();
    	
    	if (!buffer_pages.containsKey(pid)) {
    		if (buffer_pages.size()>=max_num_pages) {
    			this.evictPage();
    			//throw new DbException("Maximum number of pages in buffer already.");
    		}
    		DbFile data = Database.getCatalog().getDatabaseFile(pid.getTableId());
    		Page new_page = data.readPage(pid);
    		buffer_pages.put(pid, new_page);
    	}
    	if (perm.permLevel == 1)
    		buffer_pages.get(pid).markDirty(true, tid);
    	return buffer_pages.get(pid);
    	
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    	this.lock_manager.release_page_lock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
    	boolean out = this.lock_manager.has_lock(tid, p);
    	return out;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	for (PageId pid : this.buffer_pages.keySet()) {
    		HeapPage page = (HeapPage) this.buffer_pages.get(pid);
    		if (page.isDirty() != null && page.isDirty() == tid) {
    			if (commit)
    				flushPage(pid);
    			else
    				this.buffer_pages.put(pid, page.getBeforeImage());
    		}	
    	}
    	this.lock_manager.release_all_locks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
    	ArrayList<Page> page_list = hf.insertTuple(tid, t);
    	for (Page p : page_list) {
    		PageId pid = p.getId();
    		//make room to add page to buffer pool if full and not there
    		if (!this.buffer_pages.containsKey(pid) && this.buffer_pages.size() == max_num_pages) {
    			evictPage();
    		}
    		//p.markDirty(true, tid);
    		this.buffer_pages.put(pid, p);
    		this.buffer_pages.get(pid).markDirty(true, tid);
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	int tableId = t.getRecordId().getPageId().getTableId();
    	HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
    	ArrayList<Page> page_list = hf.deleteTuple(tid, t);
    	if (page_list != null) {
	    	for (Page p : page_list) {
	    		PageId pid = p.getId();
	    		//make room to add page to buffer pool if full and not there
	    		if (!this.buffer_pages.containsKey(pid) && this.buffer_pages.size() == max_num_pages) {
	    			evictPage();
	    		}
	    		//p.markDirty(true, tid);
	    		this.buffer_pages.put(pid, p);
	    		this.buffer_pages.get(pid).markDirty(true, tid);
	    	}
    	}
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
    	for (PageId pg : this.buffer_pages.keySet()) {
    		this.flushPage(pg);
    	}

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
     * @throws IOException 
    */
    public synchronized void discardPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
    	this.buffer_pages.remove(pid);
    	
    }
    

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     * @throws IOException 
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
    	DbFile data = Database.getCatalog().getDatabaseFile(pid.getTableId());
    	HeapPage pg = (HeapPage) this.buffer_pages.get(pid);
    	if (pg != null && pg.isDirty() != null) {
    		pg.markDirty(false, null);
    		data.writePage((Page) pg);
    		pg.setBeforeImage();
    		
    	}
    	
    	
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	for (PageId pg : this.buffer_pages.keySet()) {
    		HeapPage page = (HeapPage) this.buffer_pages.get(pg);
    		if (page.isDirty() == tid)
    			this.flushPage(pg);
    		
    	}
    	
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * @throws IOException 
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    	ArrayList<PageId> not_dirty_pages = new ArrayList<PageId>();
    	for (PageId pid : this.buffer_pages.keySet()) {
    		if (this.buffer_pages.get(pid).isDirty() == null) {
    			//System.out.println("DIRTY PAGE: "+pid);
    			//only want to evict clean pages
    			not_dirty_pages.add(pid);
    		}
    	}
    	if (not_dirty_pages.size() == 0) {
    		throw new DbException("No page was evcited");
    	}
    	//System.out.println("buffer pool"+this.max_num_pages);
    	//System.out.println(not_dirty_pages.size()+"SIZE");
    	int ind = (int) Math.floor(Math.random()*not_dirty_pages.size());
    	//System.out.println("index"+ind);
    			
    	PageId to_evict = not_dirty_pages.get((int) Math.floor(Math.random()*not_dirty_pages.size()));
    	try {
    		assert this.buffer_pages.get(to_evict).isDirty() == null : "Evicitng dirty page";
			flushPage(to_evict);
		} catch (IOException e) {
			e.printStackTrace();
		}
    	this.buffer_pages.remove(to_evict);
    	
    }

}




