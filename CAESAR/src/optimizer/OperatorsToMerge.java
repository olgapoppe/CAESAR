package optimizer;

public class OperatorsToMerge {
	
	int from;
	int to;
		
	public OperatorsToMerge (int f, int t) {
		from = f;
		to = t;		
	}
	
	public String toString() {
		return "from: " + from + " to: " + to;
	}
}
