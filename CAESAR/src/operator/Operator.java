package operator;

public abstract class Operator {
	
	double cost;
		
	Operator (double c) {
		cost = c;
	}
	
	public abstract boolean omittable(Operator neighbor); 
}
