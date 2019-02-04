package simpledb;

import java.io.*;
import java.util.*;

/**

 */
public class PageLocker {
	private HashMap<PageId, TransactionId> exclusives;
	private HashMap<PageId, Set<TransactionId>> shareds;
	
	private HashMap<TransactionId, Set<PageId>> txn_exclusives;
	private HashMap<TransactionId, Set<PageId>> txn_shareds;
	

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public PageLocker() {
        // some code goes here
    	this.exclusives = new HashMap<PageId, TransactionId>();
    	this.shareds = new HashMap<PageId, Set<TransactionId>>();
    	this.txn_exclusives = new HashMap<TransactionId, Set<PageId>>();
    	this.txn_shareds = new HashMap<TransactionId, Set<PageId>>();
    	   	
    }
    
    public boolean check_lock(TransactionId tid, PageId pid, Permissions p) {
    	boolean get_lock = false;
    	if (p.permLevel == 0) {
    		//read only case, want a shared lock
    		TransactionId ex_owner = exclusives.get(pid);
    		if (ex_owner != null && !ex_owner.equals(tid)) {
    			//there is a exclusive lock, different from current txn
    			get_lock = false;
    		}
    		else {
    			//exclusive lock == null, so can get a shared lock
    			//System.out.println("read lock acq");
    			get_lock = true;
    		}
    	}
    	else if (p.permLevel == 1) {
    		//read-write case, want exclusive lock
    		TransactionId ex_owner = exclusives.get(pid);
    		Set<TransactionId> sharing = shareds.get(pid);
    		if (ex_owner != null && !ex_owner.equals(tid)) {//exclusive lock is taken, not by same txn
    			//System.out.println("exclusive lock taken");
    			get_lock = false;
    		}
    		else {
    			//if sharing with someone other then itself --> no exclusive lock
    			if (sharing != null) {
    				if (sharing.size() > 1) { //multiple shared locks = bad
    					get_lock = false;
    					//System.out.println("no exclusive - multiple sharers");
    				}
    				else if (sharing.size() == 1 && !sharing.contains(tid)) { //txn doesn't ahve the one shared lock
    					get_lock = false;
    					//System.out.println(" no exclusive - not the one shared lock");
    				}
    				else {
    					//one shared lock and its the same txn
    					//need to remove the shared lock, but can get the exclusive lock
    					//System.out.println("got exclusive, removed shared");
    					get_lock = true;
    				}
    			}
    			else {
    				//no shared locks, can get exclusive
    				//System.out.println("got exclusive");
    				get_lock = true;
    			}
    		}
    	}	
    	else {
    		//there are only two permissions... shouldnt get here
    		get_lock = false;
    	}
    	System.out.println(get_lock);
		return get_lock;	
    }
    
    public boolean acquire_lock(TransactionId tid, PageId pid, Permissions p) {
    	//System.out.println("txn: "+ tid.getId()+ "page: "+ pid.getPageNumber()+ " perm "+p.toString());
    	boolean can_lock = check_lock(tid, pid, p);
    	if (can_lock) {
    		if (p.permLevel == 0) {
    			add_shared_lock(tid, pid);
    			return true;
    		} else {
    			add_exclusive_lock(tid, pid);
    			return true;
    		}
    	}
    	//can't lock...
    	return false;	
    }
    
    private void add_shared_lock(TransactionId tid, PageId pid) {
    	Set<TransactionId> sharing = shareds.get(pid);
    	if (sharing == null) {
    		//no shared locks yet, make new set and add it
    		HashSet<TransactionId> s = new HashSet<TransactionId>();
    		s.add(tid);
    		shareds.put(pid, s);
    	}
    	else {
    		//there already exists a shared lock, so just add to the set
    		sharing.add(tid);
    		shareds.put(pid, sharing);
    	}
    	Set<PageId> my_pages = txn_shareds.get(tid);
    	if (my_pages == null) {
    		HashSet<PageId> s = new HashSet<PageId>();
    		s.add(pid);
    		txn_shareds.put(tid, s);
    	}
    	else {
    		my_pages.add(pid);
    		txn_shareds.put(tid, my_pages);
    	}
    }
    
    private void remove_shared_lock(TransactionId tid, PageId pid) {
    	Set<TransactionId> sharing = shareds.get(pid);
    	sharing.remove(tid);
    	if (sharing.size() == 0) 
    		shareds.remove(pid);
    	else
    		shareds.put(pid, sharing);
    	Set<PageId> my_pages = txn_shareds.get(tid);
    	my_pages.remove(pid);
    	if (my_pages.size() == 0)
    		txn_shareds.remove(tid);
    	else
    		txn_shareds.put(tid, my_pages);
    }
    
    private void add_exclusive_lock(TransactionId tid, PageId pid) {
    	//remove any shared locks
    	Set<TransactionId> shared_txns = shareds.get(pid);
    	if (shared_txns != null) {
    		//there should only be one (this transaction) o/w shouldnt be getting exclusive lock
    		remove_shared_lock(tid, pid);	
    	}
    	
    	TransactionId exs = exclusives.get(pid);
    	if (exs == null) {
    		//no exclusive lock yet, as expected
    		exclusives.put(pid, tid);
    	}
    	else {
    		//this should not happen... throw an error
    		System.out.println("Adding an exclusive when there is already an owner");
    	}
    	
    	Set<PageId> my_pages = txn_exclusives.get(tid);
    	if (my_pages == null) {
    		HashSet<PageId> s = new HashSet<PageId>();
    		s.add(pid);
    		txn_exclusives.put(tid, s);
    	}
    	else {
    		my_pages.add(pid);
    		txn_exclusives.put(tid, my_pages);
    	}
    }
    
    private void remove_exclusive_lock(TransactionId tid, PageId pid) {
    	exclusives.remove(pid);
    	Set<PageId> my_pages = txn_exclusives.get(tid);
    	my_pages.remove(pid);
    	if (my_pages.size() == 0)
    		txn_exclusives.remove(tid);
    	else
    		txn_exclusives.put(tid, my_pages);
    }
    
    public void release_page_lock(TransactionId tid, PageId pid) {
    	Set<PageId> sh = txn_shareds.get(tid);
    	if (sh != null && sh.contains(pid))
    		remove_shared_lock(tid, pid);
    	Set<PageId> ex = txn_exclusives.get(tid);
    	if (ex != null && ex.contains(pid))
    		remove_exclusive_lock(tid, pid);
    }
    
    public boolean has_lock(TransactionId tid, PageId pid) {
    	Set<PageId> my_ex_pages = txn_exclusives.get(tid);
    	Set<PageId> my_sh_pages = txn_shareds.get(tid);
    	if (my_ex_pages.contains(pid) || my_sh_pages.contains(pid))
    		return true;
    	return false;
    }
    
    public void release_all_locks(TransactionId tid) {
    	Set<PageId> sh = txn_shareds.get(tid);
    	if (sh != null) {
    		for (PageId pid : sh) {
    			Set<TransactionId> shared_t = shareds.get(pid);
    			shared_t.remove(tid);
    			if (shared_t.size() == 0) shareds.remove(pid);
    			else { shareds.put(pid, shared_t); }
    		}
    		txn_shareds.remove(tid);
    	}
    	Set<PageId> ex = txn_exclusives.get(tid);
    	if (ex != null) {
    		for (PageId pid : ex) {
    			TransactionId ex_t = exclusives.get(pid);
    			if (ex_t != null) exclusives.remove(pid);
    		}
    		txn_exclusives.remove(tid);
    	}
    	
    }
}

