package optimizer;

import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import operator.*;

public class ExhaustiveSearch {
	
	public static void search (QueryPlan original_query_plan) {
		
		// Shared data structures
		LinkedBlockingQueue<QueryPlan> initial_query_plans = new LinkedBlockingQueue<QueryPlan>();
		initial_query_plans.add(original_query_plan);
    	LinkedBlockingQueue<QueryPlan> results_of_permutation = new LinkedBlockingQueue<QueryPlan>();
    	LinkedBlockingQueue<QueryPlan> results_of_merge = new LinkedBlockingQueue<QueryPlan>();
    	LinkedBlockingQueue<QueryPlan> results_of_omission = new LinkedBlockingQueue<QueryPlan>();
    	AtomicBoolean permuter_done = new AtomicBoolean(false);
    	AtomicBoolean merger_done = new AtomicBoolean(false);
    	AtomicBoolean omittor_done = new AtomicBoolean(false);
    	
    	boolean change = true;
		int iteration = 1;
		int number_of_query_plans = 0;
		double cost = 0;
		QueryPlan query_plan = new QueryPlan (new LinkedList<Operator>());
		
		while (change) {
			
			change = false;
			System.out.println("----------------------------------------\nIteration " + iteration + ".");
    	    
			// Start one thread per operation
			Omittor omittor = new Omittor(initial_query_plans, results_of_omission, omittor_done);
			Thread omittor_thread = new Thread(omittor);
			omittor_thread.start();
    	
			Merger merger = new Merger(results_of_omission, results_of_merge, omittor_done, merger_done);
			Thread merger_thread = new Thread(merger);
			merger_thread.start();
    	
			Permuter permuter = new Permuter(results_of_merge, results_of_permutation, merger_done, permuter_done);
			Thread permuter_thread = new Thread(permuter);
			permuter_thread.start(); 				
    	
			// Search result in current iteration 
			while (true) {
				if (permuter_done.get()) {					
				
					if (omittor.change || merger.change || permuter.change) {
						number_of_query_plans = permuter.number_of_query_plans;
						query_plan = permuter.cheapest_query_plan;
						cost = permuter.min_cost;
					}
					break;
			    
				} else {
					try { Thread.sleep(500); } catch (InterruptedException e) { e.printStackTrace(); }
			}}
			// Reset local variables
			change = omittor.change || merger.change || permuter.change;
			initial_query_plans.clear();
			initial_query_plans.addAll(results_of_permutation);
			results_of_omission.clear();
			omittor_done.set(false);
			results_of_merge.clear();
			merger_done.set(false);
			results_of_permutation.clear();
			permuter_done.set(false);
			iteration++;
			
			System.out.println(initial_query_plans.size());
		} 
		System.out.println("\nExhaustive search creates " + number_of_query_plans + " query plans. (" + 
				query_plan.toString() + ") is the cheapest. Its cost is " + cost);	
	}
}
