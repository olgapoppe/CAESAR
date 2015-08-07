package operator;

public class Filter extends Operator {

	Filter (double c) {
		super(c);
	}
	
	public boolean omittable (Operator neighbor) {
		return false;
	}
}
