package optimizer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import operator.Operator;

public class Permuter implements Runnable {
	
	QueryPlan input_query_plan;
	LinkedBlockingQueue<QueryPlan> output_query_plans;
	AtomicBoolean permuter_done;
	
	public Permuter (QueryPlan input, LinkedBlockingQueue<QueryPlan> output, AtomicBoolean pd) {
	
		input_query_plan = input;
		output_query_plans = output;
		permuter_done = pd;
	}
	
	public void run () {
		
		ArrayList<QueryPlan> accumulator = new ArrayList<QueryPlan>();
		permute(input_query_plan,0,accumulator);    		
	    	
		permuter_done.set(true);
		System.out.println("Permuter is done.");
	}
	
	void permute (QueryPlan query_plan, int k, ArrayList<QueryPlan> accumulator) {
			
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
	}
	
	static QueryPlan greedy_permutation (QueryPlan query_plan) {
		
		for(int i = 0; i < query_plan.operators.size(); i++) {
			int j = i;
			while (j-1>=0 && query_plan.operators.get(j).lowerable(query_plan.operators.get(j-1))) {
				Collections.swap(query_plan.operators, j, j-1);
				j--;
			}
		}
		return query_plan;	
	}
}