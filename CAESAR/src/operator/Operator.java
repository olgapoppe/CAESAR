package operator;

public abstract class Operator {
	
	public abstract boolean omittable (Operator neighbor); 
	public abstract int getCost ();
	public abstract boolean equals (Operator op);
	public abstract String toString ();
	
	public boolean mergable (Operator neighbor) {
		return false;
	}
}
