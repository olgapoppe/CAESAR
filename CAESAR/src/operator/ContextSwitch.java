package operator;

public class ContextSwitch implements Operator {
	
	String context;

	ContextSwitch (String con) {		
		context = con;
	}
	
	public boolean omittable (Operator neighbor) {
		
		// Neighbor is no context initiation
		if (!(neighbor instanceof ContextSwitch)) 
			return false;
				
		ContextSwitch other = (ContextSwitch) neighbor;
		return context.equals(other.context);
	}
	
	public int getCost() {
		return 1;
	}
}
