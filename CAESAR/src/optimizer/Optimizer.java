package optimizer;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import operator.*;

public class Optimizer {
	
	static void omit (LinkedList<Operator> query_plan) {
		
		for (int i=0; i<query_plan.size(); i++) {
			
			Operator operator = query_plan.get(i);
			Operator before = query_plan.get(i-1);
			Operator after = query_plan.get(i+1);
			if (operator.omittable(before) || operator.omittable(after)) {
				 LinkedList<Operator> new_query_plan = new LinkedList<Operator>();
				 new_query_plan.addAll(query_plan);
				 new_query_plan.remove(i);
			}
		}
		
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
