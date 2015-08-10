package optimizer;

import java.util.ArrayList;

public class Main {
	
	 public static void main(String[] args) {
	    	
	    // Original query plan
	    //String query_plan_string = "FI x>3; FI z>3; CW c; PR x, y, z; ED a; FI x>10; FI y>3; CW c; CW c; PR x, y; ED b";
		String query_plan_string = "FI x>3; CW c; PR x, y, z; ED a";
	    QueryPlan qp = QueryPlan.parse(query_plan_string);
	    System.out.println("Original query plan:\n" + qp.toString());
	    	    	
	    ArrayList<QueryPlan> query_plans_at_level_0 = new ArrayList<QueryPlan>();
	    query_plans_at_level_0.add(qp);	    
	    
	    /*** Exhaustive search ***/
	    ExhaustiveSearch es = new ExhaustiveSearch();	    
	    
	    // Operator omission 
	    System.out.println("\nResults of operator omission: ");
	    ArrayList<QueryPlan> query_plans_at_level_1 = es.search(query_plans_at_level_0,"omit");
	    for (QueryPlan nqp : query_plans_at_level_1) {
	    	System.out.println(nqp.toString() + " with cost " + nqp.getCost());
	    }
	    
	    // Operator permutation
	    System.out.println("\nResults of operator permutation: ");
	    ArrayList<QueryPlan> query_plans = new ArrayList<QueryPlan>();
	    ArrayList<QueryPlan> query_plans_at_level_2 = es.permute(qp, 0, query_plans);
	    for (QueryPlan nqp : query_plans_at_level_2) {
	    	System.out.println(nqp.toString() + " with cost " + nqp.getCost());
	    }	    
	    	
	    // Operator merge
	    System.out.println("\nResults of operator merge: ");
	    ArrayList<QueryPlan> query_plans_at_level_3 = es.search(query_plans_at_level_0,"merge");
	    for (QueryPlan nqp : query_plans_at_level_3) {
	    	System.out.println(nqp.toString() + " with cost " + nqp.getCost());
	    }	    
	    
	    /*** Optimized search ***/
	    // Operator omission
	    
	    // Operator permutation
    	
	    // Operator merge
	 }
}
