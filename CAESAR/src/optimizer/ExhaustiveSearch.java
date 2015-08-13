package optimizer;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExhaustiveSearch {
	
	public static void search (QueryPlan original_query_plan) {
		
		// Shared data structures
    	LinkedBlockingQueue<QueryPlan> results_of_permutation = new LinkedBlockingQueue<QueryPlan>();
    	LinkedBlockingQueue<QueryPlan> results_of_merge = new LinkedBlockingQueue<QueryPlan>();
    	LinkedBlockingQueue<QueryPlan> results_of_omission = new LinkedBlockingQueue<QueryPlan>();
    	AtomicBoolean permuter_done = new AtomicBoolean(false);
    	AtomicBoolean merger_done = new AtomicBoolean(false);
    	AtomicBoolean omittor_done = new AtomicBoolean(false);
    	    
    	// Start one thread per operation
    	Permuter permuter = new Permuter(original_query_plan, results_of_permutation, permuter_done);
    	Thread permuter_thread = new Thread(permuter);
    	permuter_thread.start();
    	/*results_of_permutation.add(original_query_plan);
    	permuter_done.set(true);*/
    
    	Merger merger = new Merger(results_of_permutation, results_of_merge, permuter_done, merger_done);
    	Thread merger_thread = new Thread(merger);
    	merger_thread.start();
    	
    	Omittor omittor = new Omittor(results_of_merge, results_of_omission, merger_done, omittor_done);
    	Thread omittor_thread = new Thread(omittor);
    	omittor_thread.start();
    	
    	// Search result	 
		while (true) {
			if (omittor_done.get()) {					
				
				System.out.println("\nExhaustive search creates " + omittor.number_of_options + " query plans. (" + 
				omittor.cheapest_query_plan.toString() + ") is the cheapest. Its cost is " + omittor.min_cost);				   
			    break;
			    
			} else {
				try { Thread.sleep(500); } catch (InterruptedException e) { e.printStackTrace(); }
		}}  	
	}
}
