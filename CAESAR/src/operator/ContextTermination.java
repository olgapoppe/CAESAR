package operator;

public class ContextTermination implements Operator {
	
	String context;

	ContextTermination (String con) {		
		context = con;
	}
	
	public boolean omittable (Operator neighbor) {
		
		// Neighbor is no context initiation
		if (!(neighbor instanceof ContextTermination)) 
			return false;
				
		ContextTermination other = (ContextTermination) neighbor;
		return context.equals(other.context);
	}
	
	public int getCost() {
		return 1;
	}
}
