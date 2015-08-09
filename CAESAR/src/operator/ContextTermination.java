package operator;

public class ContextTermination implements Operator {
	
	String context;

	ContextTermination (String con) {		
		context = con;
	}
	
	public boolean omittable (Operator neighbor) {
		return this.equals(neighbor);
	}
	
	public int getCost() {
		return 1;
	}
	
	public boolean equals(Operator operator) {
		
		if (!(operator instanceof ContextTermination)) return false;						
		ContextTermination other = (ContextTermination) operator;
		return context.equals(other.context);
	}
	
	public String toString() {
		return "CT " + context;
	}
}
