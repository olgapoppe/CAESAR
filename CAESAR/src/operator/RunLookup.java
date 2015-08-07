package operator;

public class RunLookup extends Operator {

	RunLookup (double c) {
		super(c);
	}
	
	public boolean omittable (Operator neighbor) {
		return false;
	}
}
