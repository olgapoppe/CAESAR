package operator;

public class ContextWindow implements Operator {
	
	String context;
	
	ContextWindow (String con) {		
		context = con;
	}

	public boolean omittable (Operator neighbor) {
		
		// Neighbor is no context window
		if (!(neighbor instanceof ContextWindow)) 
			return false;
		
		ContextWindow other = (ContextWindow) neighbor;
		
		// Neighbor is the same context window
		return context.equals(other.context);	
	}
	
	public int getCost() {
		return 1;
	}
}
