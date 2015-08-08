package operator;

import java.util.ArrayList;

public class Projection implements Operator {
	
	ArrayList<String> attributes;
	
	public Projection (ArrayList<String> a) {
		attributes = a;
	}

	public boolean omittable (Operator neighbor) {
		
		// Neighbor is no projection
		if (!(neighbor instanceof Projection)) 
			return false;
		
		Projection other = (Projection) neighbor;
		
		// Neighbor has more attributes
		if (other.attributes.size() > attributes.size()) 
			return false;
		
		// Neighbor has all attributes of this projection
		return attributes.containsAll(other.attributes);	
	}
	
	public int getCost() {
		return attributes.size();
	}
}
