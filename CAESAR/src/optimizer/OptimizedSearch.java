package optimizer;

public class OptimizedSearch {
	
	public static void search (QueryPlan query_plan) {
		
		boolean change = true;
		int iteration = 1;
		
		while (change) {
			
			change = false;
			System.out.println("----------------------------------------\nIteration " + iteration + ".");
			
			OutputOfOptimizedSearch after_omission = Omittor.greedy_omission(query_plan);
			System.out.println("Result of omission: " + after_omission.query_plan.toString());			
		
			OutputOfOptimizedSearch after_permutation = Permuter.greedy_permutation(after_omission.query_plan);
			System.out.println("Result of permutation: " + after_permutation.query_plan.toString());	
			
			OutputOfOptimizedSearch after_merge = Merger.greedy_merge(after_permutation.query_plan);
			System.out.println("Result of merge: " + after_merge.query_plan.toString() + " with cost " + after_merge.query_plan.getCost());	
			
			change = after_omission.change || after_permutation.change || after_merge.change;
			query_plan = after_merge.query_plan;
			iteration++;
		}
	}
}