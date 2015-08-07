package operator;

public class Filter extends Operator {
	
	Disjunction predicate;

	Filter (double c, Disjunction p) {
		super(c);
		predicate = p;
	}
	
	public boolean omittable (Operator neighbor) {
		
		// Neighbor is no filter
		if (!(neighbor instanceof Filter)) 
			return false;
				
		Filter other = (Filter) neighbor;
		
		return other.predicate.subsumedBy(predicate);
	}
}
