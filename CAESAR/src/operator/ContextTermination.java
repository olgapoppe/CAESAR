package operator;

public class ContextTermination extends Operator {

	ContextTermination (double c) {
		super(c);
	}
	
	public boolean omittable (Operator neighbor) {
		return false;
	}
}
