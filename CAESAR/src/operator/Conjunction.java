package operator;

import java.util.ArrayList;

public class Conjunction {
	
	public ArrayList<AtomicPredicate> atomicPredicates;
	
	public Conjunction (ArrayList<AtomicPredicate> p) {
		this.atomicPredicates = p;
	}
	
	public static Conjunction parse(String s) {
		
		ArrayList<AtomicPredicate> preds = new ArrayList<AtomicPredicate>();
		
		String allPredicates[] = s.split(" AND "); 
		if (allPredicates.length>1) {		
			for (int i=0; i<allPredicates.length; i++) {
				AtomicPredicate p = AtomicPredicate.parse(allPredicates[i]);
				preds.add(p);
			}
		} else {
			AtomicPredicate p = AtomicPredicate.parse(s);
			preds.add(p);
		}	
		return new Conjunction(preds);
	}	
	
	int getNumber() {		
		return atomicPredicates.size();
	}
	
	boolean subsumedBy (Conjunction c2) {
		for (AtomicPredicate p : c2.atomicPredicates) {
			if (!p.impliedBy(this)) { return false; }			
		}
		return true;
	}
	
	public boolean impliedBy (Disjunction d) {
		for (Conjunction c : d.conjunctivePredicates) {
			if (c.subsumedBy(this)) { return true; }
		}
		return false;
	}
	
	boolean contradicts (Conjunction c2) {
		for (AtomicPredicate p1 : this.atomicPredicates) {
			for (AtomicPredicate p2 : c2.atomicPredicates) {
				if (p1.contradicts(p2) || p2.contradicts(p1)) { return true; }
		}}
		return false;
	}
	
	boolean contradicts (Disjunction d) {
		for (Conjunction c : d.conjunctivePredicates) {
			if (!this.contradicts(c)) {
				return false;
		}}
		return true;
	}
	
	public Disjunction getNegated () {
		
		ArrayList<Conjunction> conjs = new ArrayList<Conjunction>();
		
		for (AtomicPredicate p : this.atomicPredicates) {
			
			ArrayList<AtomicPredicate> preds = new ArrayList<AtomicPredicate>();
			preds.add(p.getNegated());
			Conjunction c = new Conjunction(preds);
			conjs.add(c);
		}
		return new Disjunction(conjs);
	}
	
	public Conjunction deepCopy() {
		ArrayList<AtomicPredicate> preds = new ArrayList<AtomicPredicate>();		
		for(AtomicPredicate p : this.atomicPredicates) {
			preds.add(p.deepCopy());
		}
		return new Conjunction(preds);
	}
	
	public boolean containsMandatoryPrimaryKeyConstraint (ArrayList<String> primaryKeyAttributes) {
		
		for (AtomicPredicate p : this.atomicPredicates) {
			if (primaryKeyAttributes.contains(p.attribute)) {
				return true;
		}}
		return false;		
	}
	
	public String toString() {
		
		String s = "";
		for (AtomicPredicate pred : this.atomicPredicates) {
			
			s += pred.toString();
			if (this.atomicPredicates.indexOf(pred) != (this.atomicPredicates.size() - 1)) {
				s += " AND ";
		}}
		return s;
	}
}