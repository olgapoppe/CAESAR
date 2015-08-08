package operator;

public class Filter implements Operator {
	
	Disjunction predicate;

	Filter (Disjunction p) {
		predicate = p;
	}
	
	public boolean omittable (Operator neighbor) {
		
		// Neighbor is no filter
		if (!(neighbor instanceof Filter)) 
			return false;
				
		Filter other = (Filter) neighbor;
		
		return other.predicate.subsumedBy(predicate);
	}
	
	public int getCost() {
		return predicate.getNumber();
	}
}
