package operator;

public abstract class Operator {
	
	public double selectivity = 1;
	
	public abstract boolean omittable (Operator neighbor); 
	public abstract double getCost ();
	public abstract boolean equals (Operator op);
	public abstract String toString ();
	
	public boolean mergable (Operator neighbor) {
		return false;
	}
}
