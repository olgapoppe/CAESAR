package optimizer;

import java.util.LinkedList;
import operator.*;

public class QueryPlan {
	
	LinkedList<Operator> operators;
	
	QueryPlan(LinkedList<Operator> ops) {
		operators = ops;
	}
	
	public int getCost() {
		int cost = 0;
		for (Operator op : operators) {
			cost += op.getCost();
		}		
		return cost;
	}
}
