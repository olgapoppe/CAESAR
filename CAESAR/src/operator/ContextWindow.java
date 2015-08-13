package operator;

public class ContextWindow extends Operator {
	
	String context;
	
	public ContextWindow (String con) {		
		context = con;
	}
	
	public static ContextWindow parse(String s) {		
		
		String context = s.substring(3); // Skip "CW "				
		return new ContextWindow(context);
	}

	public boolean omittable (Operator neighbor) {		
		return this.equals(neighbor);	
	}
	
	public boolean lowerable (Operator neighbor) {
		return !((neighbor instanceof ContextInitiation) ||
				(neighbor instanceof ContextSwitch) ||
				(neighbor instanceof ContextTermination));
	}
	
	public double getCost() {
		return 1;
	}
	
	public double getSelectivity () {
		return 0.3;
	}
	
	public boolean equals(Operator operator) {
		
		if (!(operator instanceof ContextWindow)) return false;				
		ContextWindow other = (ContextWindow) operator;	
		return context.equals(other.context);
	}
	
	public String toString() {
		return "CW " + context;
	}
}
