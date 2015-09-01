package window;

import java.util.ArrayList;

public class WindowDistribution {
	
	public static void main (String args[]) {
		
		int lambda = 10;
		int expensive_window_number = 5;
		
		int lastSec = 100;
		int window_length = 1;
		
		int count = 0;
		
		while (count < 10) {
			System.out.println(expensive_window_number + " poisson numbers with lambda " + lambda + " are: " + getPoissonNumbers(lambda,expensive_window_number));
			count++;
		}
		
		while (count < 10) {
			System.out.println(expensive_window_number + " uniform numbers are: " + getUniformNumbers(lastSec,expensive_window_number,window_length));
			count++;
		}	
	}
	
	public static ArrayList<Integer> getPoissonNumbers (double lambda, int expensive_window_number) {
		
		ArrayList<Integer> results = new ArrayList<Integer> ();
			
		while (results.size() < expensive_window_number) {
			
			int result = getPoisson(lambda);
			if (!results.contains(result)) results.add(result);	
		}
		return results;
	}
	
	public static int getPoisson (double lambda) {
		
		double L = Math.exp(-lambda);
		double p = 1.0;
		int k = 0;

		do {
			k++;
		    p *= Math.random();
		} while (p > L);

		return k - 1;
	}
	
	public static ArrayList<Integer> getUniformNumbers (int lastSec, int expensive_window_number, int window_length) {
		  
		ArrayList<Integer> results = new ArrayList<Integer>();
		
		int total_window_number = lastSec/window_length;
		int cheap_window_number_inbetween = total_window_number/(expensive_window_number+1) - 1;
		
		int current_window = cheap_window_number_inbetween;
		int count = 0;
		
		while (count < expensive_window_number) {
			
			int result = current_window + 1;
			results.add(result);
			current_window += cheap_window_number_inbetween;
			count++;
		}		
		return results;
	}
}
