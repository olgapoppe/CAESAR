package operator;

public class RunCreation implements Operator {
	
	public boolean omittable (Operator neighbor) {
		
		// Neighbor is no context initiation
		return (neighbor instanceof RunCreation);
	}
	
	public int getCost() {
		return 1;
	}
}
