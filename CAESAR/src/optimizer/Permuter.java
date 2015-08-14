package optimizer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import operator.*;

public class Permuter implements Runnable {
	
	LinkedBlockingQueue<QueryPlan> input_query_plans;
	LinkedBlockingQueue<QueryPlan> output_query_plans;
	AtomicBoolean previous_done;
	AtomicBoolean permuter_done;
	
	ArrayList<QueryPlan> accumulator;
	double min_cost;
	public QueryPlan cheapest_query_plan;
	int number_of_query_plans;
	
	public Permuter (LinkedBlockingQueue<QueryPlan> input, LinkedBlockingQueue<QueryPlan> output, AtomicBoolean prd, AtomicBoolean pd) {
	
		input_query_plans = input;
		output_query_plans = output;
		previous_done = prd;
		permuter_done = pd;
		
		accumulator = new ArrayList<QueryPlan>();
		min_cost = Double.MAX_VALUE;
		cheapest_query_plan = new QueryPlan(new LinkedList<Operator>());
		number_of_query_plans = 0;
	}
	
	public void run () {
		
		while (!(previous_done.get() && input_query_plans.isEmpty())) {			
			if (input_query_plans.peek()!=null) {
				
				QueryPlan qp = input_query_plans.poll();
				ArrayList<QueryPlan> qps = new ArrayList<QueryPlan>();
				qps.add(qp);
				exhaustive_search(qps);				
	    		
	    	} else {
	    		try { Thread.sleep(500); } catch (InterruptedException e) { e.printStackTrace(); }
	    	}		
		}	    	
		permuter_done.set(true);
		System.out.println("Permuter is done.");
	}
	
	/**
	 * Finds all alternative query plans that arise due to operator reordering and 
	 * adds them to the accumulator and the output.
	 * Finds the cheapest query plan and its cost.
	 * Computes the total number of alternative query plans.
	 * @param qps			input query plans
	 */
	void exhaustive_search (ArrayList<QueryPlan> qps) {
		
		for (QueryPlan qp : qps) {			
			
			if (!qp.contained(accumulator,false)) {
				
				// Base case: Add this query plan to the result
				accumulator.add(qp);
				output_query_plans.add(qp);
				double cost = qp.getCost();
				System.out.println("Result of reordering: " + qp.toString() + " with cost " + cost);	
				
				if (cost<min_cost) {
		    		min_cost = cost;
		    		cheapest_query_plan = qp;
		    	}
				number_of_query_plans++;
				
				// Recursive case: Merge operators in this query plan
				exhaustive_search(exhaustive_reordering(qp));
		}}				
	}
	
	/**
	 * Finds all alternative query plans that arise due to operator permutation.
	 * @param qps			input query plans
	 * @return resulting alternative query plans
	 */
	ArrayList<QueryPlan> exhaustive_reordering (QueryPlan query_plan) {
		
		ArrayList<QueryPlan> new_query_plans = new ArrayList<QueryPlan>();
			
		for(int i = 0; i < query_plan.operators.size(); i++) {
			if (i-1>=0 && query_plan.operators.get(i).lowerable(query_plan.operators.get(i-1),false)) {
				LinkedList<Operator> new_ops = new LinkedList<Operator>();
				new_ops.addAll(query_plan.operators);
				Collections.swap(new_ops, i, i-1);
				QueryPlan new_query_plan = new QueryPlan(new_ops);	
				new_query_plans.add(new_query_plan);			
		}}
		return new_query_plans;	
	}

	/*void permute (QueryPlan query_plan, int k, ArrayList<QueryPlan> accumulator) {
			
		for(int i = k; i < query_plan.operators.size(); i++) {
			Collections.swap(query_plan.operators, i, k);
	        permute(query_plan, k+1, accumulator);
	        Collections.swap(query_plan.operators, k, i);
	    }
	    if (k == query_plan.operators.size()-1) {
	      	LinkedList<Operator> new_operators = new LinkedList<Operator>();
	       	new_operators.addAll(query_plan.operators);
	       	QueryPlan nqp = new QueryPlan(new_operators);
	       	if (!nqp.contained(accumulator)) {
	       		accumulator.add(nqp);
	       		output_query_plans.add(nqp);
	       		System.out.println("Result of permutation: " + nqp.toString() + " with cost " + nqp.getCost());
	    }}	    
	}*/
	
	/**
	 * All operators in the query plan that can be pushed down, 
	 * are pushed down to the lowest possible position.
	 * @param query_plan input query plan
	 * @return resulting query plan
	 */
	static OutputOfOptimizedSearch greedy_permutation (QueryPlan query_plan) {
		
		boolean change = false;
		
		for(int i = 0; i < query_plan.operators.size(); i++) {
			int j = i;
			while (j-1>=0 && query_plan.operators.get(j).lowerable(query_plan.operators.get(j-1),true)) {
				Collections.swap(query_plan.operators, j, j-1);
				j--;
				change = true;
		}}
		return new OutputOfOptimizedSearch(query_plan, change);	
	}
}