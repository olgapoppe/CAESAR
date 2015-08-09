package operator;

public class RunDeletion implements Operator {
	
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
