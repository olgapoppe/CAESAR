package optimizer;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class Main {
	
	 public static void main(String[] args) {
		 
		double start = System.currentTimeMillis()/new Double(1000);
		
		// Original query plan
	    //String query_plan_string = "FI x>3; FI z>3; CW c; PR x, y, z; ED a; FI x>10; FI y>3; CW c; PR x, y; ED b";
		//String query_plan_string = "FI x>3; FI z>3; CW c; PR x, y, z; ED a";
		String query_plan_string = "";
		for (String s : args) {
			query_plan_string += s + " ";
		}
		
	    QueryPlan original_query_plan = QueryPlan.parse(query_plan_string);
	    System.out.println("Original query plan: " + original_query_plan.toString());
	    	    	
	    // Shared data structures
	    LinkedBlockingQueue<QueryPlan> results_of_omission = new LinkedBlockingQueue<QueryPlan>();
	    LinkedBlockingQueue<QueryPlan> results_of_permutation = new LinkedBlockingQueue<QueryPlan>();
	    LinkedBlockingQueue<QueryPlan> results_of_merge = new LinkedBlockingQueue<QueryPlan>();
	    AtomicBoolean omittor_done = new AtomicBoolean(false);
	    AtomicBoolean permuter_done = new AtomicBoolean(false);
	    AtomicBoolean merger_done = new AtomicBoolean(false);
	    	    
	    /*** Exhaustive search ***/
	    Omittor omittor = new Omittor(original_query_plan, results_of_omission, omittor_done);
	    Thread omittor_thread = new Thread(omittor);
	    omittor_thread.start();
	    
	    Permuter permuter = new Permuter(results_of_omission, results_of_permutation, omittor_done, permuter_done);
	    Thread permuter_thread = new Thread(permuter);
	    permuter_thread.start();
	    
	    Merger merger = new Merger(results_of_permutation, results_of_merge, permuter_done, merger_done);
	    Thread merger_thread = new Thread(merger);
	    merger_thread.start();	       
	    
	    /*** Optimized search ***/
	    // Operator omission
	    
	    // Operator permutation
    	
	    // Operator merge
	    
	    /*** Search result ***/	 
		while (true) {
			if (merger_done.get()) {
				
				System.out.println("\nExhaustive search creates " + merger.number_of_options + " query plans." + 
				" The cheapest of them is (" + merger.cheapest_query_plan.toString() + ") with cost " + merger.min_cost);
			    
			    /*** Duration of search ***/
			    double end = System.currentTimeMillis()/new Double(1000);
			    double duration = end - start;
			    System.out.println("\nDuration: " + duration);
			    break;
			    
			} else {
				try { Thread.sleep(500); } catch (InterruptedException e) { e.printStackTrace(); }
			}
		}   
		System.out.println("Main is done.");
	 }
}
