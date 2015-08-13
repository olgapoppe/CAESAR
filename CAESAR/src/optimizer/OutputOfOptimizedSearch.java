package optimizer;

public class OutputOfOptimizedSearch {
	
	public QueryPlan query_plan;
	public boolean change;	
	
	OutputOfOptimizedSearch (QueryPlan qp, boolean c) {
		query_plan = qp;
		change = c;
	}
}