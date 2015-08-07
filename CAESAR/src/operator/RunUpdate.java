package operator;

public class RunUpdate extends Operator {
	
	RunUpdate (double c) {
		super(c);
	}
	
	public boolean omittable (Operator neighbor) {
		return false;
	}

}
