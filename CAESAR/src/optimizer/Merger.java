package optimizer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import operator.*;

public class Merger implements Runnable {
	
	LinkedBlockingQueue<QueryPlan> input_query_plans;
	LinkedBlockingQueue<QueryPlan> output_query_plans;
	AtomicBoolean previous_done;
	AtomicBoolean merger_done;
	
	ArrayList<QueryPlan> accumulator;
	
	public Merger (LinkedBlockingQueue<QueryPlan> input, LinkedBlockingQueue<QueryPlan> output, AtomicBoolean pd, AtomicBoolean md) {
		
		input_query_plans = input;
		output_query_plans = output;
		previous_done = pd;
		merger_done = md;
		
		accumulator = new ArrayList<QueryPlan>();
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
		merger_done.set(true);
		System.out.println("Merger is done.");
	}
	
	/**
	 * Finds all alternative query plans that arise due to operator merge and 
	 * adds them to the accumulator and the output.
	 * @param qps			input query plans
	 */
	void exhaustive_search (ArrayList<QueryPlan> qps) {
		
		for (QueryPlan qp : qps) {			
			
			if (!qp.contained(accumulator)) {
				
				// Base case: Add this query plan to the result
				accumulator.add(qp);
				output_query_plans.add(qp);
				System.out.println("Result of merge: " + qp.toString() + " with cost " + qp.getCost());
				
				// Recursive case: Merge operators in this query plan
				exhaustive_search(exhaustive_merge(qp));
		}}				
	}
	
	/**
	 * For each operator in the query plan: 
	 * If it can be merged with the successor, 
	 * new query plan is created where these operators are merged.
	 * @param query_plan input query plan
	 * @return resulting query plans
	 */
	ArrayList<QueryPlan> exhaustive_merge (QueryPlan query_plan) {
		
		ArrayList<QueryPlan> new_query_plans = new ArrayList<QueryPlan>();
				
		for (int i=0; i<query_plan.operators.size(); i++) {
			
			Operator operator = query_plan.operators.get(i);
			boolean a;
			if (i+1<query_plan.operators.size()) {
				Operator after = query_plan.operators.get(i+1);
				a = operator.mergable(after);
			} else {
				a = false;
			}			
			if (a) {
				LinkedList<Operator> new_ops = new LinkedList<Operator>();
				for (int j=0; j<query_plan.operators.size(); j++) {
					if (j!=i && j!=i+1) {
						new_ops.add(query_plan.operators.get(j));
					} else {
						if (j==i) {
							Filter fi1 = (Filter) query_plan.operators.get(i);
							Filter fi2 = (Filter) query_plan.operators.get(i+1);
							Filter merged_filter = fi1.merge(fi2,false);
							new_ops.add(merged_filter);
				}}}
				QueryPlan new_query_plan = new QueryPlan(new_ops);	
				new_query_plans.add(new_query_plan);				
		}}
		return new_query_plans;
	}
	
	/**
	 * All operators in the query plan that can be merged, are merged.
	 * @param query_plan input query plan
	 * @return resulting query plan
	 */
	static OutputOfOptimizedSearch greedy_merge (QueryPlan query_plan) {		
		
		LinkedList<Operator> new_operators = new LinkedList<Operator>();
		ArrayList<OperatorsToMerge> ops2merge = query_plan.getOperators2merge();
		boolean change = !ops2merge.isEmpty();
		int i = 0;
		
		for (OperatorsToMerge toMerge : ops2merge) {
			
			// Before operators to merge
			while (i<toMerge.from) {
				new_operators.add(query_plan.operators.get(i));
				i++;
			}
			// Merged filters
			ArrayList<Filter> filters2merge = new ArrayList<Filter>();
			while (i<=toMerge.to) {
				Filter fi = (Filter) query_plan.operators.get(i);
				filters2merge.add(fi);
				i++;
			}
			Filter mergedFilter = Filter.mergeAll(filters2merge,true);
			new_operators.add(mergedFilter);			
			
			i = toMerge.to+1;			
		}
		// After operators to merge
		while (i<query_plan.operators.size()) {
			new_operators.add(query_plan.operators.get(i));
			i++;
		}		
		return new OutputOfOptimizedSearch(new QueryPlan(new_operators),change);	
	}
}
