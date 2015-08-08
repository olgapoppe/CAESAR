package operator;

public class ContextInitiation implements Operator {
	
	String context;
	
	ContextInitiation (String con) {		
		context = con;
	}
	
	public boolean omittable (Operator neighbor) {
		
		// Neighbor is no context initiation
		if (!(neighbor instanceof ContextInitiation)) 
			return false;
				
		ContextInitiation other = (ContextInitiation) neighbor;
		return context.equals(other.context);
	}

	public int getCost() {
		return 1;
	}
}
