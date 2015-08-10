package optimizer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import operator.*;

public class ExhaustiveSearch {
	
	/*** Operator omission ***/
	ArrayList<QueryPlan> omit (QueryPlan query_plan) {
		
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
	
	/*** Operator permutation ***/
	 ArrayList<QueryPlan> permute (QueryPlan query_plan, int k, ArrayList<QueryPlan> new_query_plans) {
			
		for(int i = k; i < query_plan.operators.size(); i++) {
            Collections.swap(query_plan.operators, i, k);
            permute(query_plan, k+1, new_query_plans);
            Collections.swap(query_plan.operators, k, i);
        }
        if (k == query_plan.operators.size()-1) {
        	LinkedList<Operator> new_operators = new LinkedList<Operator>();
        	new_operators.addAll(query_plan.operators);
        	new_query_plans.add(new QueryPlan(new_operators));
        }
        return new_query_plans;
    }  
	
	/*** Operator merge ***/
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
	
	/*** Recursive calls ***/
	ArrayList<QueryPlan> search (ArrayList<QueryPlan> qps, String method) {
		
		ArrayList<QueryPlan> results = new ArrayList<QueryPlan>();
		
		for (QueryPlan qp : qps) {
			
			results.add(qp);
			
			// Base case: Omit operators in this query plan
			ArrayList<QueryPlan> new_query_plans;
			if (method.equals("omit")) {
				new_query_plans = omit(qp);
			} else {
				new_query_plans = merge(qp);
			}
			for (QueryPlan nqp : new_query_plans) {
				if (!nqp.contained(results)) results.add(nqp);
			}			
			// Recursive case: Omit operators in newly produced query plans
			ArrayList<QueryPlan> more_new_query_plans = search(new_query_plans,method);
			for (QueryPlan nqp : more_new_query_plans) {
				if (!nqp.contained(results)) results.add(nqp);
			}
		}		
		return results;		
	}	
	
	ArrayList<QueryPlan> permute_all (ArrayList<QueryPlan> qps) {
		
		ArrayList<QueryPlan> results = new ArrayList<QueryPlan>();
		
		for (QueryPlan qp : qps) {
			ArrayList<QueryPlan> accumulator = new ArrayList<QueryPlan>();
			results.addAll(permute(qp,0,accumulator));
		}		
		return results;		
	}
}
