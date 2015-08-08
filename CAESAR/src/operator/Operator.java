package operator;

public interface Operator {
	
	public boolean omittable (Operator neighbor); 
	public int getCost ();
}
