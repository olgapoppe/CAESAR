package operator;

public interface Operator {
	
	public boolean omittable (Operator neighbor); 
	public int getCost ();
	public boolean equals (Operator op);
	public String toString ();
}
