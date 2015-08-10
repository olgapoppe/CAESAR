package operator;

public class ContextInitiation extends Operator {
	
	String context;
	
	ContextInitiation (String con) {		
		context = con;
	}
	
	public static ContextInitiation parse(String s) {		
		
		String context = s.substring(3); // Skip "CI "				
		return new ContextInitiation(context);
	}
	
	public boolean omittable (Operator neighbor) {
		return this.equals(neighbor);
	}

	public int getCost() {
		return 1;
	}
	
	public boolean equals(Operator operator) {
		
		if (!(operator instanceof ContextInitiation)) return false;						
		ContextInitiation other = (ContextInitiation) operator;
		return context.equals(other.context);
	}
	
	public String toString() {
		return "CI " + context;
	}
}
