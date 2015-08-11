package optimizer;

import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import operator.Operator;

public class Permuter implements Runnable {
	
	LinkedBlockingQueue<QueryPlan> input_query_plans;
	LinkedBlockingQueue<QueryPlan> output_query_plans;
	AtomicBoolean omittor_done;
	AtomicBoolean permuter_done;
	
	public Permuter (LinkedBlockingQueue<QueryPlan> input, LinkedBlockingQueue<QueryPlan> output, AtomicBoolean od, AtomicBoolean pd) {
	
		input_query_plans = input;
		output_query_plans = output;
		omittor_done = od;
		permuter_done = pd;
	}
	
	public void run () {
		
		while (!(omittor_done.get() && input_query_plans.isEmpty())) {			
			if (input_query_plans.peek()!=null) {
				
	    		QueryPlan qp = input_query_plans.poll();
	    		permute(qp,0);    		
	    		
	    	} else {
	    		try { Thread.sleep(500); } catch (InterruptedException e) { e.printStackTrace(); }
	    	}		
		}	
		permuter_done.set(true);
		System.out.println("Permuter is done.");
	}
	
	void permute (QueryPlan query_plan, int k) {
			
		for(int i = k; i < query_plan.operators.size(); i++) {
			Collections.swap(query_plan.operators, i, k);
	        permute(query_plan, k+1);
	        Collections.swap(query_plan.operators, k, i);
	    }
	    if (k == query_plan.operators.size()-1) {
	      	LinkedList<Operator> new_operators = new LinkedList<Operator>();
	       	new_operators.addAll(query_plan.operators);
	       	QueryPlan nqp = new QueryPlan(new_operators);
	       	output_query_plans.add(nqp);
	       	System.out.println("Result of permutation: " + nqp.toString() + " with cost " + nqp.getCost());
	    }	    
	}
}