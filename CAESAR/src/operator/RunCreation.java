package operator;

public class RunCreation implements Operator {
	
	public static RunCreation parse(String s) {
		return new RunCreation();
	}
	
	public boolean omittable (Operator neighbor) {		
		return this.equals(neighbor);
	}
	
	public int getCost() {
		return 1;
	}
	
	public boolean equals (Operator operator) {
		return (operator instanceof RunCreation);		
	}
	
	public String toString() {
		return "RC";
	}
}
