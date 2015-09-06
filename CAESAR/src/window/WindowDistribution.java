package window;

import java.util.ArrayList;

public class WindowDistribution {
	
	public static void main (String args[]) {
		
		/*** Input parameters ***/	
		int lastSec = 180;
		int window_length = 50;	
		int expensive_window_number = 2;
		
		int window_center = 80;
		int lambda = window_center/window_length + 1;
		System.out.println("Central window is: " + lambda);
		
		/*** Uniform window distribution ***/
		System.out.println(getTimeIntervalsForUniformDistribution(lastSec, window_length, expensive_window_number));
		
		/*** Poisson window distribution ***/
		int count = 0;
		while (count < 5) {
			System.out.println(	expensive_window_number + " poisson numbers with lambda " + lambda + " are: " + 
								getTimeIntervalsForPoissonDistribution(lastSec, window_length, expensive_window_number, lambda));
			count++;
		}
	}
	
	public static ArrayList<TimeInterval> getTimeIntervalsForPoissonDistribution (double lastSec, int window_length, int expensive_window_number, int lambda) {
		
		/*** Get expensive windows ***/
		ArrayList<Integer> expensive_windows = getPoissonNumbers(lastSec, window_length, expensive_window_number, lambda);
		
		/*** Get expensive time intervals ***/
		ArrayList<TimeInterval> results = new ArrayList<TimeInterval>();
		for (Integer expensive_window : expensive_windows) {
			
			double start = new Double(expensive_window * window_length + 1).intValue();
			double end = new Double((expensive_window + 1) * window_length).intValue();
			
			TimeInterval i = new TimeInterval(start,end);
			results.add(i);			
		}		
		return results;
	}
	
	public static ArrayList<Integer> getPoissonNumbers (double lastSec, int window_length, int expensive_window_number, double lambda) {
		
		ArrayList<Integer> results = new ArrayList<Integer> ();
		int total_window_number = new Double(lastSec).intValue()/window_length - 1;
			
		while (results.size() < expensive_window_number) {
			
			int result = getPoisson(lambda);
			if (!results.contains(result) && result <= total_window_number) results.add(result);
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
	
	public static ArrayList<TimeInterval> getTimeIntervalsForUniformDistribution (double lastSec, int window_length, int expensive_window_number) {
		 	
		/*** Get cheap window length ***/
		double expensive_time = expensive_window_number * window_length;
		double cheap_time = lastSec - expensive_time;
		double cheap_window_length = cheap_time/(expensive_window_number + 1); 
		
		/*** Get expensive time intervals ***/
		ArrayList<TimeInterval> results = new ArrayList<TimeInterval>();
		double window_bound = cheap_window_length;
		while (results.size() < expensive_window_number) {
			
			double start = new Double(window_bound).intValue();
			double end = new Double(window_bound + window_length).intValue();
			
			TimeInterval i = new TimeInterval(start,end);
			results.add(i);
			
			window_bound += window_length + cheap_window_length;
		}			
		return results;
	}
}
