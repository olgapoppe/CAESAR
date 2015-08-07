package operator;

public class RunCreation extends Operator {
	
	RunCreation (double c) {
		super(c);
	}
	
	public boolean omittable (Operator neighbor) {
		return false;
	}

}
