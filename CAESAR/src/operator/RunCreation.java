package operator;

public class RunCreation extends Operator {
	
	public static RunCreation parse(String s) {
		return new RunCreation();
	}
	
	public boolean omittable (Operator neighbor) {		
		return (neighbor instanceof RunDeletion) || this.equals(neighbor);
	}
	
	public double getCost() {
		return 1;
	}
	
	public boolean equals (Operator operator) {
		return (operator instanceof RunCreation);		
	}
	
	public String toString() {
		return "RC";
	}
}
