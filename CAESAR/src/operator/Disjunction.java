package operator;

import java.util.ArrayList;

public class Disjunction {
	
	public ArrayList<Conjunction> conjunctivePredicates;
	
	Disjunction (ArrayList<Conjunction> p) {
		this.conjunctivePredicates = p;
	}
	
	boolean subsumedBy (Disjunction d2) {
		for (Conjunction c : d2.conjunctivePredicates) {
			if (!c.impliedBy(this)) { return false; }			
		}
		return true;
	}
	
	boolean contradicts (Disjunction d2) {
		if (this.conjunctivePredicates.isEmpty() || d2.conjunctivePredicates.isEmpty()) {
			return true;
		} else {			
		
		if (this.conjunctivePredicates.size()<2 && d2.conjunctivePredicates.size()<2) {			
			return this.conjunctivePredicates.get(0).contradicts(d2.conjunctivePredicates.get(0));
		} else {			
		
		for (Conjunction c1 : this.conjunctivePredicates) {
			for (Conjunction c2 : d2.conjunctivePredicates) {
				
				//System.out.println(c1.toString());
				//System.out.println(c2.toString());
				if (!c1.contradicts(c2)) { return false; }
		}}
		return true;
		}}		
	}
	
	public Disjunction getNegated () {	
			
		ArrayList<Disjunction> disjs = new ArrayList<Disjunction>();				
			
		for (Conjunction c : this.conjunctivePredicates) {
			Disjunction d = c.getNegated();
			disjs.add(d);
		}
		Disjunction d1 = disjs.get(0);
		disjs.remove(0);
		return d1.getCNF(disjs);		
	}
	
	public Disjunction getCNF(ArrayList<Disjunction> disjs) {
		
		Disjunction d1 = this;
		int i = 0;
		
		while (disjs.size() > i) {
			
			Disjunction d2 = disjs.get(i);
			ArrayList<Conjunction> conjs = new ArrayList<Conjunction>();
			
			for (Conjunction c1 : d1.conjunctivePredicates) {
				for (Conjunction c2 : d2.conjunctivePredicates) {
					
					ArrayList<AtomicPredicate> preds = new ArrayList<AtomicPredicate>();
					preds.addAll(c1.atomicPredicates);
					preds.addAll(c2.atomicPredicates);
					Conjunction c = new Conjunction(preds);						
					conjs.add(c);
			}}
			d1 = new Disjunction(conjs);			
			i++;			
		}	
		return d1;
	}
	
	public boolean containsMandatoryPrimaryKeyConstraint (ArrayList<String> primaryKeyAttributes) {
		
		for (Conjunction c : this.conjunctivePredicates) {
			if (!c.containsMandatoryPrimaryKeyConstraint(primaryKeyAttributes)) {
				return false;
		}}
		return true;		
	}
	
	public Disjunction deepCopy() {
		ArrayList<Conjunction> conjs = new ArrayList<Conjunction>();		
		for(Conjunction c : this.conjunctivePredicates) {
			conjs.add(c.deepCopy());
		}
		return new Disjunction(conjs);
	}
	
	public String toString() {
		
		String s = "";
		for (Conjunction conj : this.conjunctivePredicates) {
			
			s += conj.toString();
			if (this.conjunctivePredicates.indexOf(conj) != (this.conjunctivePredicates.size() - 1)) {
				s += " OR \n";
			} else {
				s += "";
			}
		}
		return s;
	}
}
