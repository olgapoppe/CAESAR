package optimizer;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class GreedySearch {
	
	public static void search (QueryPlan solution) {
	
		// Data structures
		ArrayList<QueryPlan> chosen = new ArrayList<QueryPlan>();
		chosen.add(solution);
		Double min_cost = solution.getCost();				
		
		ArrayList<QueryPlan> results_of_permutation = new ArrayList<QueryPlan>();
		ArrayList<QueryPlan> results_of_merge = new ArrayList<QueryPlan>();
		ArrayList<QueryPlan> results_of_omission = new ArrayList<QueryPlan>();
		AtomicBoolean permuter_done = new AtomicBoolean(false);
		AtomicBoolean merger_done = new AtomicBoolean(false);
		AtomicBoolean omittor_done = new AtomicBoolean(false);
		 
		// Counters
		int iteration = 1;	
		int number_of_query_plans = 0;
						
		while (!chosen.isEmpty()) {
					
			System.out.println("----------------------------------------\nIteration " + iteration + ".");
		    	    
			// Start one thread per operation
			Omittor omittor = new Omittor(chosen, results_of_omission, omittor_done);
			Thread omittor_thread = new Thread(omittor);
			omittor_thread.start();
		    	
			Merger merger = new Merger(chosen, results_of_merge, merger_done);
			Thread merger_thread = new Thread(merger);
			merger_thread.start();
		    	
			Permuter permuter = new Permuter(chosen, results_of_permutation, permuter_done);
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
			// Get all children				
			ArrayList<QueryPlan> considered = new ArrayList<QueryPlan> ();
			considered.addAll(results_of_omission);
			considered.addAll(results_of_merge);
			considered.addAll(results_of_permutation);
			considered.removeAll(chosen);
			
			// Get children with minimal cost			
			chosen.clear();			
			for (QueryPlan new_qp : considered) {
				double cost = new_qp.getCost();
				if (cost < min_cost && !new_qp.contained(chosen, false)) {
					
					min_cost = cost;
					chosen.clear();
					chosen.add(new_qp);					
					solution = new_qp;
					
					number_of_query_plans = 1;
				}
				if (cost == min_cost && !new_qp.contained(chosen, false)) {
					
					chosen.add(new_qp);					
					
					number_of_query_plans++;
				}
			}									
			results_of_omission.clear();
			omittor_done.set(false);
			results_of_merge.clear();
			merger_done.set(false);
			results_of_permutation.clear();
			permuter_done.set(false);
			iteration++;	
				 
			// Print the result of this iteration
			System.out.println(	"\nGreedy search creates " + number_of_query_plans + " query plans. (" + 
								solution.toString() + ") is the cheapest. Its cost is " + min_cost);
		}
	}
}