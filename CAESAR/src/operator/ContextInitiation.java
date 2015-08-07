package operator;

public class ContextInitiation extends Operator {
	
	ContextInitiation (double c) {
		super(c);
	}
	
	public boolean omittable (Operator neighbor) {
		return false;
	}

}
