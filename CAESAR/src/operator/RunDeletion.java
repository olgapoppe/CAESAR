package operator;

public class RunDeletion extends Operator {
	
	public static RunDeletion parse(String s) {
		return new RunDeletion();
	}
	
	public boolean omittable (Operator neighbor) {		
		return this.equals(neighbor);
	}
	
	public int getCost() {
		return 1;
	}
	
	public boolean equals (Operator operator) {
		return (operator instanceof RunDeletion);		
	}
	
	public String toString() {
		return "RD";
	}
}
