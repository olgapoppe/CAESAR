package optimizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import operator.*;

public class Optimizer {
	
	static ArrayList<QueryPlan> omit (QueryPlan query_plan) {
		
		ArrayList<QueryPlan> new_query_plans = new ArrayList<QueryPlan>();
		
		for (int i=0; i<query_plan.operators.size(); i++) {
			
			Operator operator = query_plan.operators.get(i);
			Operator before = query_plan.operators.get(i-1);
			Operator after = query_plan.operators.get(i+1);
			if (operator.omittable(before) || operator.omittable(after)) {
				 QueryPlan new_query_plan = new QueryPlan(query_plan.operators);
				 new_query_plan.operators.remove(i);
				 new_query_plans.add(new_query_plan);
			}
		}
		return new_query_plans;
	}
	
	static void permute (LinkedList<Operator> arr, int k) {
        for(int i = k; i < arr.size(); i++){
            Collections.swap(arr, i, k);
            permute(arr, k+1);
            Collections.swap(arr, k, i);
        }
        if (k == arr.size()-1){
            System.out.println(Arrays.toString(arr.toArray()));
        }
    }
    public static void main(String[] args) {
    	
    	LinkedList<Operator> query_plan = new LinkedList<Operator>();
    	
    	
    	
    	//Optimizer.permute(Arrays.asList(1,2,3), 0);
    }
}
