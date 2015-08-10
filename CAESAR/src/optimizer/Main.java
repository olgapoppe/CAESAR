package optimizer;

import java.util.ArrayList;

public class Main {
	
	 public static void main(String[] args) {
	    	
	    // Original query plan
	    String query_plan_string = "FI x>3; CW c; PR x, y, z; ED a; FI x>10; CW c; PR x, y; ED b";
	    QueryPlan qp = QueryPlan.parse(query_plan_string);
	    System.out.println("Original query plan:\n" + qp.toString());
	    	    	
	    ArrayList<QueryPlan> query_plans = new ArrayList<QueryPlan>();
	    query_plans.add(qp);
	    
	    /*** Exhaustive search ***/
	    // Operator omission
	    ExhaustiveSearch es = new ExhaustiveSearch();
	    ArrayList<QueryPlan> new_query_plans = es.omit(query_plans);
	    	
	    System.out.println("After operator omission: ");
	    for (QueryPlan nqp : new_query_plans) {
	    	System.out.println(nqp.toString() + " with cost " + nqp.getCost());
	    }
	    
	    // Operator permutation
	    	
	    // Operator merge
	    
	    /*** Optimized search ***/
	    // Operator omission
	    
	    // Operator permutation
    	
	    // Operator merge
	 }
}
