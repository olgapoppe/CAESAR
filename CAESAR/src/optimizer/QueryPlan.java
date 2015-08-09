package optimizer;

import java.util.ArrayList;
import java.util.LinkedList;
import operator.*;

public class QueryPlan {
	
	LinkedList<Operator> operators;
	
	public QueryPlan(LinkedList<Operator> ops) {
		operators = ops;
	}
	
	public int getCost() {
		int cost = 0;
		for (Operator op : operators) {
			cost += op.getCost();
		}		
		return cost;
	}
	
	public boolean contained (ArrayList<QueryPlan> list) {
		for (QueryPlan qp : list) {
			if (this.equals(qp)) return true;
		}
		return false;
	}
	
	public boolean equals (QueryPlan other) {
		
		if (operators.size()!=other.operators.size()) return false;
		for (int i=0; i<operators.size(); i++) {
			if (!operators.get(i).equals(other.operators.get(i))) 
				return false;	
		}
		return true;
	}
	
	public String toString() {
		String s = "";
		for (Operator op : operators) {
			s += op.toString() + " ";
		}
		return s;
	}
}
