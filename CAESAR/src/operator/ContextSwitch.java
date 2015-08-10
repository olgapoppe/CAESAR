package operator;

public class ContextSwitch implements Operator {
	
	String context;

	ContextSwitch (String con) {		
		context = con;
	}
	
	public static ContextSwitch parse(String s) {		
		
		String context = s.substring(3); // Skip "CS "				
		return new ContextSwitch(context);
	}
	
	public boolean omittable (Operator neighbor) {
		return this.equals(neighbor);
	}
	
	public int getCost() {
		return 1;
	}
	
	public boolean equals(Operator operator) {
		
		if (!(operator instanceof ContextSwitch)) return false;						
		ContextSwitch other = (ContextSwitch) operator;
		return context.equals(other.context);
	}
	
	public String toString() {
		return "CS " + context;
	}
}
