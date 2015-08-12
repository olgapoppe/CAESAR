package optimizer;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class Main {
	
	/***
	 * Input parameters:
	 * 1) 0 for exhaustive search, 1 for optimized search
	 * 2) original query plan
	 */
	
	 public static void main(String[] args) {
		 
		// Optimized or exhaustive search
		boolean optimized = (Integer.parseInt(args[0])==1);
		
		// Original query plan
	    //String query_plan_string = "FI x>3; FI z>3; CW c; PR x, y, z; ED a; FI x>10; FI y>3; CW c; PR x, y; ED b";
		//String query_plan_string = "FI x>3; FI z>3; CW c; PR x, y, z; ED a";
		String query_plan_string = "";
		for (int i=1; i<args.length; i++) {
			query_plan_string += args[i] + " ";
		}
		
	    QueryPlan original_query_plan = QueryPlan.parse(query_plan_string);
	    System.out.println("Original query plan: " + original_query_plan.toString());
	    
	    // Start the timer
	    double start = System.currentTimeMillis()/new Double(1000);
	    
	    /*** Exhaustive search ***/
	    if (!optimized) {
	    	    	
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
	    /*** Optimized search ***/
	    } else {
	    	
	    	QueryPlan after_omission = Omittor.greedy_omission(original_query_plan);
	    	System.out.println("Result of omission: " + after_omission.toString());	 
	    	
	    	QueryPlan after_merge = Merger.greedy_merge(after_omission);
	    	System.out.println("Result of merge: " + after_merge.toString());	    	   		    	
	    }
	    
	    /*** Duration of search ***/
	    double end = System.currentTimeMillis()/new Double(1000);
	    double duration = end - start;
	    System.out.println("\nDuration: " + duration + ". Main is done.");
	 }
}
