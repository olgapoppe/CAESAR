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
		String s = optimized ? "Optimized search." : "Exhaustive search.";
		System.out.println(s);
		
		/*** Original query plan ***/	   
		String query_plan_string = "";
		for (int i=1; i<args.length; i++) {
			query_plan_string += args[i] + " ";
		}
		//System.out.println("|" + query_plan_string + "|");		
		
	    QueryPlan solution = QueryPlan.parse(query_plan_string);
	    System.out.println("----------------------------------------\nOriginal query plan (" + solution.toString() + 
	    		")\nhas " + solution.operators.size() +
	    		" operators and cost " + solution.getCost() +
	    		"\n----------------------------------------");
	    
	    /*** Start the timer ***/
	    double start = System.currentTimeMillis()/new Double(1000);
	    
	    /*** Search ***/
	    if (!optimized) {	    	    	
	    	ExhaustiveSearch.search(solution); 	    
	    } else {	    	
	    	GreedySearch.search(solution);    	   		    	
	    }
	    
	    /*** Duration of search ***/
	    double end = System.currentTimeMillis()/new Double(1000);
	    double duration = end - start;
	    System.out.println("\nDuration: " + duration + ". Main is done.");
	 }
}
