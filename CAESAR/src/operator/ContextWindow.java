package operator;

public class ContextWindow extends Operator {
	
	String context;
	
	ContextWindow (double c, String con) {
		super(c);
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
}
