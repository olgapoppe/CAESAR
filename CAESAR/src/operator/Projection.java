package operator;

import java.util.ArrayList;

public class Projection extends Operator {
	
	ArrayList<String> attributes;
	
	public Projection (ArrayList<String> a) {
		attributes = a;
	}

	public static Projection parse(String s) {		
		
		ArrayList<String> attributes = new ArrayList<String>();
		String attributes_string = s.substring(3); // Skip "PR "	
		String allAttributes[] = attributes_string.split(", ");
		for (String attribute : allAttributes) {
			attributes.add(attribute);
		}				
		return new Projection(attributes);
	}

	public boolean omittable (Operator neighbor) {
		
		if (!(neighbor instanceof Projection)) return false;		
		
		Projection other = (Projection) neighbor;
		if (other.attributes.size() > attributes.size()) return false;		
		return attributes.containsAll(other.attributes);	
	}
	
	public boolean lowerable (Operator neighbor) {
		
		if (neighbor instanceof ContextWindow) return false;
				
		if (neighbor instanceof Projection) {
			Projection other = (Projection) neighbor;
			return this.attributes.containsAll(other.attributes);
		}
		
		if (neighbor instanceof Filter) {
			Filter other = (Filter) neighbor;
			return this.attributes.containsAll(other.predicate.getAttributes()) && !other.lowerable(this);
		}
		
		if (neighbor instanceof RunUpdate) {
			RunUpdate other = (RunUpdate) neighbor;
			return this.attributes.containsAll(other.getAttributes());
		}		
		return true;		
	}
	
	public boolean equals(Operator operator) {
		
		if (!(operator instanceof Projection)) 	return false;
		Projection other = (Projection) operator;
		return attributes.containsAll(other.attributes) && other.attributes.containsAll(attributes);
	}
	
	public double getCost() {
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
