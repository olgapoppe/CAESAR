package operator;

public class ContextSwitch extends Operator {

	ContextSwitch (double c) {
		super(c);
	}
	
	public boolean omittable (Operator neighbor) {
		return false;
	}
}
