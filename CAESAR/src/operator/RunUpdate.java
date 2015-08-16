package operator;

import java.util.ArrayList;

public class RunUpdate extends Operator {
	
	ArrayList<Tuple> tuples;
	
	RunUpdate (ArrayList<Tuple> t) {
		tuples = t;
	}
	
	public static RunUpdate parse(String s) {
		
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		String tuples_string = s.substring(3); // Skip "RU "	
		String allTuples[] = tuples_string.split(", ");
		for (String tuple_string : allTuples) {
			Tuple tuple = Tuple.parse(tuple_string);
			tuples.add(tuple);
		}				
		return new RunUpdate(tuples);
	}
	
	public boolean omittable (Operator neighbor) {
		return (neighbor instanceof RunDeletion) || this.equals(neighbor);		
	}
	
	public boolean mergable (Operator neighbor) {		
		return (neighbor instanceof RunUpdate);		
	}
	
	public Operator merge (Operator other, boolean optimized) {
		RunUpdate ru = (RunUpdate) other;
		ArrayList<Tuple> tups = new ArrayList<Tuple>();
		tups.addAll(ru.tuples);
		for (Tuple t : tuples) {
			if (!ru.containsValueFor(t.attribute)) tups.add(t);
		}
		return new RunUpdate(tups);		
	}
	
	public static Operator mergeAll (ArrayList<Operator> list, boolean optimized) {
		Operator result = list.get(0);
		for (int i=1; i<list.size(); i++) {
			result = result.merge(list.get(i), optimized);
		}
		return result;
	}
	
	public ArrayList<String> getAttributes() {
		ArrayList<String> attributes = new ArrayList<String>();
		for (Tuple t : tuples) {
			attributes.add(t.attribute);
		}
		return attributes;
	}	
	
	public boolean containsValueFor (String attr) {
		for (Tuple t : tuples) {
			if (t.attribute.equals(attr)) return true;
		}
		return false;
	}
	
	public double getCost() {
		return tuples.size();
	}
	
	public boolean equals (Operator operator) {
		if (!(operator instanceof RunUpdate)) return false;
		RunUpdate other = (RunUpdate) operator;
		return tuples.containsAll(other.tuples) && other.tuples.containsAll(tuples);		
	}
	
	public String toString() {
		String s = "RU ";
		for (Tuple t : tuples) {
			s += t.toString() + ", ";
		}
		return s;
	}
}
