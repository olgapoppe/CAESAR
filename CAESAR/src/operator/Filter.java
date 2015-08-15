package operator;

import java.util.ArrayList;

public class Filter extends Operator {
	
	public Disjunction predicate;

	public Filter (Disjunction p) {
		predicate = p;
	}
	
	public static Filter parse(String s) {		
		
		String predicate_string = s.substring(3); // Skip "FI "
		Disjunction predicate = Disjunction.parse(predicate_string);		
		return new Filter(predicate);
	}
	
	public boolean omittable (Operator neighbor) {
		
		if (!(neighbor instanceof Filter)) return false;				
		Filter other = (Filter) neighbor;		
		return other.predicate.subsumedBy(predicate);
	}
	
	public boolean mergable (Operator neighbor) {		
		return (neighbor instanceof Filter);		
	}
	
	public Operator merge (Operator other, boolean optimized) {
		Filter fi = (Filter) other;
		if (optimized) {
			if (this.predicate.subsumedBy(fi.predicate)) { // this is more specific
				return this;
			} else {
			if (fi.predicate.subsumedBy(this.predicate)) { // other is more specific
				return other;
		}}}
		ArrayList<Disjunction> disjs = new ArrayList<Disjunction>();
		disjs.add(fi.predicate);
		Disjunction predicate = this.predicate.getCNF(disjs);
		Filter merged_filter = new Filter(predicate);
		return merged_filter;		
	}
	
	public static Operator mergeAll (ArrayList<Filter> list, boolean optimized) {
		Operator result = list.get(0);
		for (int i=1; i<list.size(); i++) {
			result = result.merge(list.get(i), optimized);
		}
		return result;
	}
	
	public boolean lowerable (Operator neighbor, boolean optimized) {
		if (optimized) {
			return 	(neighbor instanceof Projection);
		} else {
			return 	(neighbor instanceof Filter) ||
					(neighbor instanceof ContextWindow) ||
					(neighbor instanceof Projection);
		}
	}
	
	public double getCost() {
		return predicate.getNumber();
	}
	
	public double getSelectivity () {
		return Math.pow(0.5,predicate.getNumber());
	}
	
	public boolean equals (Operator operator) {		
		if (!(operator instanceof Filter)) return false;				
		Filter other = (Filter) operator;
		return predicate.equals(other.predicate);				
	}
	
	public boolean equivalent (Operator operator) {
		if (!(operator instanceof Filter)) return false;				
		Filter other = (Filter) operator;
		return predicate.equivalent(other.predicate);
	}
	
	public String toString() {
		return "FI " + predicate.toString();
	}
}
