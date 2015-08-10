package optimizer;

import java.util.ArrayList;
import java.util.LinkedList;
import operator.*;

public class Main {
	
	 public static void main(String[] args) {
		 
		double start = System.currentTimeMillis()/new Double(1000);
		
		double min_cost = Double.MAX_VALUE;
		QueryPlan cheapest_query_plan = new QueryPlan(new LinkedList<Operator>());
	    	
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
	    ArrayList<QueryPlan> query_plans_at_level_2 = es.permute_all(query_plans_at_level_1);
	    for (QueryPlan nqp : query_plans_at_level_2) {
	    	System.out.println(nqp.toString() + " with cost " + nqp.getCost());
	    }	    
	    	
	    // Operator merge
	    System.out.println("\nResults of operator merge: ");
	    ArrayList<QueryPlan> query_plans_at_level_3 = es.search(query_plans_at_level_2,"merge");
	    for (QueryPlan nqp : query_plans_at_level_3) {
	    	double cost = nqp.getCost();
	    	System.out.println(nqp.toString() + " with cost " + cost);
	    	if (cost<min_cost) {
	    		min_cost = cost;
	    		cheapest_query_plan = nqp;
	    	}
	    }	    
	    
	    /*** Optimized search ***/
	    // Operator omission
	    
	    // Operator permutation
    	
	    // Operator merge
	    
	    /*** Search result ***/	    
	    System.out.println("\nCheapest query plan: " + cheapest_query_plan.toString() + " with cost " + min_cost);
	    
	    /*** Duration of search ***/
	    double end = System.currentTimeMillis()/new Double(1000);
	    double duration = end - start;
	    System.out.println("\nDuration: " + duration);
	 }
}
