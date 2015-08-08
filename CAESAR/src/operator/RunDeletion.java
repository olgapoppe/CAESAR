package operator;

public class RunDeletion implements Operator {
	
	public boolean omittable (Operator neighbor) {
		
		// Neighbor is no context initiation
		return (neighbor instanceof RunDeletion);
	}
	
	public int getCost() {
		return 1;
	}
}
