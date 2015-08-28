package optimizer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import operator.*;

public class Merger implements Runnable {
	
	ArrayList<QueryPlan> input_query_plans;
	ArrayList<QueryPlan> output_query_plans;
	AtomicBoolean merger_done;
	
	ArrayList<QueryPlan> accumulator;
	
	public Merger (ArrayList<QueryPlan> input, ArrayList<QueryPlan> output, AtomicBoolean md) {
		
		input_query_plans = input;
		output_query_plans = output;
		merger_done = md;
		
		accumulator = new ArrayList<QueryPlan>();
	}
	
	public void run () {
		
		exhaustive_search(input_query_plans);    	
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
			if (!qp.contained(accumulator,false)) {
				
				// Base case: Add this query plan to the result
				accumulator.add(qp);
				output_query_plans.add(qp);
				System.out.println("Result of merge: " + qp.toString() + " with cost " + qp.getCost());
				
				// Recursive case: Merge operators in this query plan
				if (!qp.merge_done) {
					exhaustive_search(exhaustive_merge(qp));
					qp.merge_done = true;
		}}}				
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
							Operator op1 = query_plan.operators.get(i);
							Operator op2 = query_plan.operators.get(i+1);
							Operator merged_op = op1.merge(op2,false);
							new_ops.add(merged_op);
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
			// Merged operators
			ArrayList<Operator> mergeableOperators = new ArrayList<Operator>();
			boolean filter = false;
			while (i<=toMerge.to) {
				Operator op = query_plan.operators.get(i);
				filter = (op instanceof Filter);
				mergeableOperators.add(op);
				i++;
			}
			Operator mergedOperator;
			if (filter) {
				mergedOperator = Filter.mergeAll(mergeableOperators,true);
			} else {
				mergedOperator = RunUpdate.mergeAll(mergeableOperators,true);
			}			
			new_operators.add(mergedOperator);			
			
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
