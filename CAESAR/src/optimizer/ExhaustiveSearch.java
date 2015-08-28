package optimizer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import operator.*;

public class ExhaustiveSearch {
	
	public static void search (QueryPlan original_query_plan) {
		
		// Shared data structures
		int prev_number_of_query_plans = 0;
		ArrayList<QueryPlan> initial_query_plans = new ArrayList<QueryPlan>();
		initial_query_plans.add(original_query_plan);
		ArrayList<QueryPlan> results_of_permutation = new ArrayList<QueryPlan>();
		ArrayList<QueryPlan> results_of_merge = new ArrayList<QueryPlan>();
		ArrayList<QueryPlan> results_of_omission = new ArrayList<QueryPlan>();
    	AtomicBoolean permuter_done = new AtomicBoolean(false);
    	AtomicBoolean merger_done = new AtomicBoolean(false);
    	AtomicBoolean omittor_done = new AtomicBoolean(false);
    	
    	int iteration = 1;	
				
		while (prev_number_of_query_plans < initial_query_plans.size()) {
			
			System.out.println("----------------------------------------\nIteration " + iteration + ".");
    	    
			// Start one thread per operation
			Omittor omittor = new Omittor(initial_query_plans, results_of_omission, omittor_done);
			Thread omittor_thread = new Thread(omittor);
			omittor_thread.start();
    	
			Merger merger = new Merger(initial_query_plans, results_of_merge, merger_done);
			Thread merger_thread = new Thread(merger);
			merger_thread.start();
    	
			Permuter permuter = new Permuter(initial_query_plans, results_of_permutation, permuter_done);
			Thread permuter_thread = new Thread(permuter);
			permuter_thread.start(); 				
    	
			// Wait till all operations are completed 
			while (true) {
				if (omittor_done.get() && merger_done.get() && permuter_done.get()) {						
					break;			    
				} else {
					try { Thread.sleep(500); } catch (InterruptedException e) { e.printStackTrace(); }
				}
			}
			// Reset local variables	
			prev_number_of_query_plans = initial_query_plans.size();
			initial_query_plans.clear();			
			int number_of_query_plans = 0;
			Double min_cost = Double.MAX_VALUE;
			QueryPlan cheapest_query_plan = new QueryPlan (new LinkedList<Operator>());
			
			ArrayList<QueryPlan> all_new_results = new ArrayList<QueryPlan> ();
			all_new_results.addAll(results_of_omission);
			all_new_results.addAll(results_of_merge);
			all_new_results.addAll(results_of_permutation);
			
			// Add all non-duplicate results and compute the best query plan seen so far
			for (QueryPlan new_qp : all_new_results) {
				if (!new_qp.contained(initial_query_plans, false)) {
					initial_query_plans.add(new_qp);
					double cost = new_qp.getCost();
					if (cost < min_cost) {
			    		min_cost = cost;
			    		cheapest_query_plan = new_qp;
			    	}
					number_of_query_plans++;
			}}			
						
			results_of_omission.clear();
			omittor_done.set(false);
			results_of_merge.clear();
			merger_done.set(false);
			results_of_permutation.clear();
			permuter_done.set(false);
			iteration++;	
		 
			// Print the result of this iteration
			System.out.println(	"\nExhaustive search creates " + number_of_query_plans + " query plans. (" + 
								cheapest_query_plan.toString() + ") is the cheapest. Its cost is " + min_cost);	
		}
	}	
}
