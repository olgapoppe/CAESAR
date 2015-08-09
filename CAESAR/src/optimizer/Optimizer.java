package optimizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import operator.*;

public class Optimizer {
	
	static ArrayList<QueryPlan> omit (QueryPlan query_plan) {
		
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
	
	static ArrayList<QueryPlan> omit (ArrayList<QueryPlan> qps) {
		
		ArrayList<QueryPlan> results = new ArrayList<QueryPlan>();
		
		for (QueryPlan qp : qps) {
			
			results.add(qp);
			
			// Base case: Omit operators in this query plan
			ArrayList<QueryPlan> new_query_plans = Optimizer.omit(qp);
			for (QueryPlan nqp : new_query_plans) {
				if (!nqp.contained(results)) results.add(nqp);
			}			
			// Recursive case: Omit operators in newly produced query plans
			ArrayList<QueryPlan> more_new_query_plans = Optimizer.omit(new_query_plans);
			for (QueryPlan nqp : more_new_query_plans) {
				if (!nqp.contained(results)) results.add(nqp);
			}
		}		
		return results;		
	}
	
	static void permute (LinkedList<Operator> arr, int k) {
        for(int i = k; i < arr.size(); i++){
            Collections.swap(arr, i, k);
            permute(arr, k+1);
            Collections.swap(arr, k, i);
        }
        if (k == arr.size()-1){
            System.out.println(Arrays.toString(arr.toArray()));
        }
    }
	
    public static void main(String[] args) {
    	
    	// First query: FI1 x>3 CW1 c PR1 x,y,z ED1 a
    	
    	AtomicPredicate p1 = new AtomicPredicate("x", ">", 3);
    	ArrayList<AtomicPredicate> ps1 = new ArrayList<AtomicPredicate>();
    	ps1.add(p1);
    	Conjunction c1 = new Conjunction(ps1);
    	ArrayList<Conjunction> cs1 = new ArrayList<Conjunction>();
    	cs1.add(c1);
    	Disjunction d1 = new Disjunction(cs1);
    	Filter fi1 = new Filter(d1);
    	
    	ContextWindow cw1 = new ContextWindow("c");
    	
    	ContextWindow cw3 = new ContextWindow("c");
    	
    	ArrayList<String> attr1 = new ArrayList<String>();
    	attr1.add("x");
    	attr1.add("y");
    	attr1.add("z");
    	Projection pr1 = new Projection(attr1);
    	
    	EventDerivation ed1 = new EventDerivation("a");
    	
    	// Second query: FI2 x>10 CW2 c PR2 x,y ED2 b
    	
    	AtomicPredicate p2 = new AtomicPredicate("x", ">", 10);
    	ArrayList<AtomicPredicate> ps2 = new ArrayList<AtomicPredicate>();
    	ps2.add(p2);
    	Conjunction c2 = new Conjunction(ps2);
    	ArrayList<Conjunction> cs2 = new ArrayList<Conjunction>();
    	cs2.add(c2);
    	Disjunction d2 = new Disjunction(cs2);
    	Filter fi2 = new Filter(d2);
    	
    	ContextWindow cw2 = new ContextWindow("c");
    	
    	ArrayList<String> attr2 = new ArrayList<String>();
    	attr2.add("x");
    	attr2.add("y");
    	Projection pr2 = new Projection(attr2);
    	
    	EventDerivation ed2 = new EventDerivation("b");
    	
    	EventDerivation ed3 = new EventDerivation("d");
    	
    	
    	LinkedList<Operator> ops = new LinkedList<Operator>();
    	ops.add(fi1);
    	ops.add(cw1);
    	ops.add(cw3);
    	ops.add(pr1);
    	ops.add(ed1);
    	ops.add(fi2);
    	ops.add(cw2);
    	ops.add(pr2);
    	ops.add(ed2);
    	ops.add(ed3);
    	
    	QueryPlan qp = new QueryPlan(ops);
    	
    	/*******************************************************************/
    	
    	System.out.println("Original query plan:\n" + qp.toString());
    	
    	ArrayList<QueryPlan> query_plans = new ArrayList<QueryPlan>();
    	query_plans.add(qp);
    			
    	ArrayList<QueryPlan> new_query_plans = Optimizer.omit(query_plans);
    	
    	System.out.println("After operator omission: ");
    	for (QueryPlan nqp : new_query_plans) {
    		System.out.println(nqp.toString());
    	}
    }
}
