package operator;

public class RunDeletion extends Operator {
	
	RunDeletion (double c) {
		super(c);
	}
	
	public boolean omittable (Operator neighbor) {
		return false;
	}
}
