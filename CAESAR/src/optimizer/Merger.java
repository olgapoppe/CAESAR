package optimizer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import operator.*;

public class Merger implements Runnable {
	
	LinkedBlockingQueue<QueryPlan> input_query_plans;
	LinkedBlockingQueue<QueryPlan> output_query_plans;
	AtomicBoolean permuter_done;
	AtomicBoolean merger_done;
	
	double min_cost;
	public QueryPlan cheapest_query_plan;
	int number_of_options;
	
	public Merger (LinkedBlockingQueue<QueryPlan> input, LinkedBlockingQueue<QueryPlan> output, AtomicBoolean od, AtomicBoolean pd) {
		
		input_query_plans = input;
		output_query_plans = output;
		permuter_done = od;
		merger_done = pd;
		
		min_cost = Double.MAX_VALUE;
		cheapest_query_plan = new QueryPlan(new LinkedList<Operator>());
		number_of_options = 0;
	}
	
	public void run () {
		
		while (!(permuter_done.get() && input_query_plans.isEmpty())) {			
			if (input_query_plans.peek()!=null) {
				
				QueryPlan qp = input_query_plans.poll();
				ArrayList<QueryPlan> qps = new ArrayList<QueryPlan>();
				qps.add(qp);
				ArrayList<QueryPlan> accumulator = new ArrayList<QueryPlan>();
				search(qps,accumulator);				
	    		
	    	} else {
	    		try { Thread.sleep(500); } catch (InterruptedException e) { e.printStackTrace(); }
	    	}		
		}	
		merger_done.set(true);
		System.out.println("Merger is done.");
	}
	
	void search (ArrayList<QueryPlan> qps, ArrayList<QueryPlan> accumulator) {
		
		for (QueryPlan qp : qps) {
			
			// Base case: Add this query plan to the result
			if (!qp.contained(accumulator)) {
				
				accumulator.add(qp);
				output_query_plans.add(qp);
				double cost = qp.getCost();
				System.out.println("Result of merge: " + qp.toString() + " with cost " + cost);
				
				if (cost<min_cost) {
		    		min_cost = cost;
		    		cheapest_query_plan = qp;
		    	}
				number_of_options++;
			}				
			// Recursive case: Merge operators in this query plan
			search(merge(qp), accumulator);			
		}				
	}
	
	ArrayList<QueryPlan> merge (QueryPlan query_plan) {
		
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
							ArrayList<Disjunction> disjs = new ArrayList<Disjunction>();
							disjs.add(fi2.predicate);
							Disjunction predicate = fi1.predicate.getCNF(disjs);
							Filter merged_filter = new Filter(predicate);
							new_ops.add(merged_filter);
				}}}
				QueryPlan new_query_plan = new QueryPlan(new_ops);	
				new_query_plans.add(new_query_plan);
		}}
		return new_query_plans;
	}
}
