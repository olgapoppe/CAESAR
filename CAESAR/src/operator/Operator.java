package operator;

public abstract class Operator {
	
	public abstract boolean omittable (Operator neighbor); 
	public abstract double getCost ();
	public abstract boolean equals (Operator op);
	public abstract String toString ();
	
	public boolean equivalent(Operator operator) {
		return this.equals(operator);
	}
	
	public boolean mergable (Operator neighbor) {
		return false;
	}	
	
	public Operator merge (Operator other, boolean optimized) {
		return this;
	}
	
	public boolean lowerable (Operator neighbor, boolean optimized) {
		return false;
	}
	
	public double getSelectivity () {
		return 1;
	}
}
