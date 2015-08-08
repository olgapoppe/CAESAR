package operator;

import java.util.ArrayList;

public class RunUpdate implements Operator {
	
	ArrayList<Tuple> tuples;
	
	RunUpdate (ArrayList<Tuple> t) {
		tuples = t;
	}
	
	public boolean omittable (Operator neighbor) {
		
		// Neighbor is no context initiation
		if (!(neighbor instanceof RunUpdate)) 
		return false;
						
		RunUpdate other = (RunUpdate) neighbor;
		return tuples.containsAll(other.tuples) && other.tuples.containsAll(tuples);
	}
	
	public int getCost() {
		return tuples.size();
	}
}
