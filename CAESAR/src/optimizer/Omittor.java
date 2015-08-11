package optimizer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import operator.Operator;

public class Omittor implements Runnable {
	
	QueryPlan input_query_plan;
	LinkedBlockingQueue<QueryPlan> output_query_plans;
	AtomicBoolean done;
	
	public Omittor (QueryPlan input, LinkedBlockingQueue<QueryPlan> output, AtomicBoolean d) {
		input_query_plan = input;
		output_query_plans = output;
		done = d;
	}
	
	public void run () {
		
		ArrayList<QueryPlan> qps = new ArrayList<QueryPlan>();
		qps.add(input_query_plan);
		ArrayList<QueryPlan> accumulator = new ArrayList<QueryPlan>();
		search(qps,accumulator);		
		done.set(true);
		System.out.println("Omittor is done.");
	}
	
	void search (ArrayList<QueryPlan> qps, ArrayList<QueryPlan> accumulator) {
				
		for (QueryPlan qp : qps) {
			
			// Base case: Add this query plan to the result
			if (!qp.contained(accumulator)) {
				accumulator.add(qp);
				output_query_plans.add(qp); 
				System.out.println("Result of omission: " + qp.toString() + " with cost " + qp.getCost());
			}				
			// Recursive case: Omit operators in this query plan
			search(omit(qp), accumulator);			
		}				
	}
	
	ArrayList<QueryPlan> omit(QueryPlan query_plan) {
		
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
}
