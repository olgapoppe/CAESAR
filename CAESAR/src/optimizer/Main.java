package optimizer;

public class Main {
	
	/***
	 * Input parameters:
	 * 1) 0 for exhaustive search, 1 for optimized search
	 * 2) original query plan
	 */
	
	 public static void main(String[] args) {
		 
		/*** Optimized or exhaustive search ***/
		boolean optimized = (Integer.parseInt(args[0])==1);
		
		/*** Original query plan ***/	   
		String query_plan_string = "";
		for (int i=1; i<args.length; i++) {
			query_plan_string += args[i] + " ";
		}		
	    QueryPlan original_query_plan = QueryPlan.parse(query_plan_string);
	    System.out.println("Original query plan (" + original_query_plan.toString() + 
	    		")\nhas " + original_query_plan.operators.size() +
	    		" operators and cost " + original_query_plan.getCost());
	    
	    /*** Start the timer ***/
	    double start = System.currentTimeMillis()/new Double(1000);
	    
	    /*** Search ***/
	    if (!optimized) {	    	    	
	    	ExhaustiveSearch.search(original_query_plan); 	    
	    } else {	    	
	    	OptimizedSearch.search(original_query_plan);    	   		    	
	    }
	    
	    /*** Duration of search ***/
	    double end = System.currentTimeMillis()/new Double(1000);
	    double duration = end - start;
	    System.out.println("\nDuration: " + duration + ". Main is done.");
	 }
}
