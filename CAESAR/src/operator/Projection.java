package operator;

import java.util.ArrayList;

public class Projection implements Operator {
	
	ArrayList<String> attributes;
	
	public Projection (ArrayList<String> a) {
		attributes = a;
	}

	public boolean omittable (Operator neighbor) {
		
		if (!(neighbor instanceof Projection)) return false;		
		
		Projection other = (Projection) neighbor;
		if (other.attributes.size() > attributes.size()) return false;		
		return attributes.containsAll(other.attributes);	
	}
	
	public boolean equals(Operator operator) {
		
		if (!(operator instanceof Projection)) 	return false;
		Projection other = (Projection) operator;
		return attributes.containsAll(other.attributes) && other.attributes.containsAll(attributes);
	}
	
	public int getCost() {
		return attributes.size();
	}
	
	public String toString() {
		String s = "PR ";
		for (String a : attributes) {
			s += a + ",";
		}		
		return s;
	}
}
