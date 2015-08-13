package optimizer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import operator.Operator;

public class Omittor implements Runnable {
	
	LinkedBlockingQueue<QueryPlan> input_query_plans;
	LinkedBlockingQueue<QueryPlan> output_query_plans;
	AtomicBoolean merger_done;
	AtomicBoolean omittor_done;
	
	double min_cost;
	public QueryPlan cheapest_query_plan;
	int number_of_options;
	
	public Omittor (LinkedBlockingQueue<QueryPlan> input, LinkedBlockingQueue<QueryPlan> output, AtomicBoolean md, AtomicBoolean od) {
		
		input_query_plans = input;
		output_query_plans = output;
		merger_done = md;
		omittor_done = od;
		
		min_cost = Double.MAX_VALUE;
		cheapest_query_plan = new QueryPlan(new LinkedList<Operator>());
		number_of_options = 0;
	}
	
	public void run () {
		
		while (!(merger_done.get() && input_query_plans.isEmpty())) {			
			if (input_query_plans.peek()!=null) {
		
				QueryPlan qp = input_query_plans.poll();
				ArrayList<QueryPlan> qps = new ArrayList<QueryPlan>();
				qps.add(qp);
				ArrayList<QueryPlan> accumulator = new ArrayList<QueryPlan>();
				exhaustive_search(qps,accumulator);			
		
	    	} else {
	    		try { Thread.sleep(500); } catch (InterruptedException e) { e.printStackTrace(); }
	    	}		
		}	
		omittor_done.set(true);
		System.out.println("Omittor is done.");
	}
	
	/**
	 * Finds all alternative query plans that arise due to operator omission.
	 * Finds the cheapest query plan and its cost.
	 * Computes the total number of alternative query plans.
	 * @param qps			input query plans 
	 * @param accumulator	resulting query plans found so far
	 */
	void exhaustive_search (ArrayList<QueryPlan> qps, ArrayList<QueryPlan> accumulator) {
				
		for (QueryPlan qp : qps) {
			
			// Base case: Add this query plan to the result
			if (!qp.contained(accumulator)) {
				accumulator.add(qp);
				output_query_plans.add(qp); 
				double cost = qp.getCost();
				System.out.println("Result of omission: " + qp.toString() + " with cost " + cost);
				
				if (cost<min_cost) {
		    		min_cost = cost;
		    		cheapest_query_plan = qp;
		    	}
				number_of_options++;
			}				
			// Recursive case: Omit operators in this query plan
			exhaustive_search(exhaustive_omission(qp), accumulator);			
		}				
	}
	
	/**
	 * For each operator in the query plan: 
	 * If it can be omitted, 
	 * new query plan is created where this operator is skipped.
	 * @param query_plan input query plan
	 * @return resulting query plans
	 */
	ArrayList<QueryPlan> exhaustive_omission (QueryPlan query_plan) {
		
		ArrayList<QueryPlan> new_query_plans = new ArrayList<QueryPlan>();
		
		for (int i=0; i<query_plan.operators.size(); i++) {
			
			Operator operator = query_plan.operators.get(i);
			boolean b;
			if (i-1>=0) {
				Operator before = query_plan.operators.get(i-1);
				b = operator.omittable(before);				
			} else {
				b = false;
			}
			boolean a;
			if (i+1<query_plan.operators.size()) {
				Operator after = query_plan.operators.get(i+1);
				a = operator.omittable(after);				
			} else {
				a = false;
			}			
			if (b || a) {
				LinkedList<Operator> new_ops = new LinkedList<Operator>();
				for (int j=0; j<query_plan.operators.size(); j++) {
					if (j!=i) new_ops.add(query_plan.operators.get(j));
				}
				QueryPlan new_query_plan = new QueryPlan(new_ops);	
				new_query_plans.add(new_query_plan);
			}
		}
		return new_query_plans;
	}
	
	/**
	 * All operators in the query plan that can be omitted, are skipped.
	 * @param query_plan input query plan
	 * @return resulting query plan
	 */
	static OutputOfOptimizedSearch greedy_omission (QueryPlan query_plan) {
		
		LinkedList<Operator> new_ops = new LinkedList<Operator>();
		QueryPlan new_query_plan = new QueryPlan(new_ops);
		boolean change = false;
				
		for (int i=0; i<query_plan.operators.size(); i++) {
			
			Operator operator = query_plan.operators.get(i);
			boolean b;
			if (i-1>=0) {
				Operator before = query_plan.operators.get(i-1);
				b = operator.omittable(before) && !operator.equals(before);	// account for duplicates, only one of them is skipped				
			} else {
				b = false;
			}
			boolean a;
			if (i+1<query_plan.operators.size()) {
				Operator after = query_plan.operators.get(i+1);
				a = operator.omittable(after);					
			} else {
				a = false;
			}			
			if (!b && !a) {
				new_ops.add(operator);			
			} else {
				change = true;
			}
		}
		return new OutputOfOptimizedSearch(new_query_plan,change);
	}		
}
