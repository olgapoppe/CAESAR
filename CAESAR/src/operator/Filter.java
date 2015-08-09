package operator;

public class Filter implements Operator {
	
	Disjunction predicate;

	public Filter (Disjunction p) {
		predicate = p;
	}
	
	public boolean omittable (Operator neighbor) {
		
		if (!(neighbor instanceof Filter)) return false;				
		Filter other = (Filter) neighbor;		
		return other.predicate.subsumedBy(predicate);
	}
	
	public int getCost() {
		return predicate.getNumber();
	}
	
	public boolean equals(Operator operator) {
		
		if (!(operator instanceof Filter)) return false;				
		Filter other = (Filter) operator;
		return predicate.equals(other.predicate);
	}
	
	public String toString() {
		return "FI " + predicate.toString();
	}
}
