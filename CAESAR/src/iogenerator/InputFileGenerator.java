package iogenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;
import event.*;

/**
 * Input file generator parses the input file and
 * copies a given number of lines to the output file. 
 * 
 * @author olga
 */
public class InputFileGenerator {
	
	public static void main (String[] args) {
	
		Scanner input = null;
		try {		
			/*** Input file ***/
			//File input_file = new File("src/input/datafile20seconds.dat");
			File input_file = new File("../../Dropbox/LR/InAndOutput/1xway/input7.dat");
			input = new Scanner(input_file);  			
					
			/*** Output file ***/
            File output_file = new File("../../input.dat");
            BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
            
            // Integer.parseInt(args[0]);
            // 357853 events are within the first for 1500 seconds 
            
            /*** Call method ***/
            
            
            
            /*** Clean-up ***/
       		input.close();
       		output.close();        		
        
		} catch (IOException e) { System.err.println(e); }		  
	}
	
	
	/**
	 * Copy position reports from input to output and change their xway to the given value 
	 * @param input
	 * @param output
	 * @param new xway
	 */
	public void changeXway (Scanner input, BufferedWriter output, int newXway) {
		
		
	}
	
	/**
	 * Copy position reports that have the given direction from input to output
	 * @param input
	 * @param output
	 * @param dir
	 */
	public void selectTuples (Scanner input, BufferedWriter output, int dir) {
		
		String eventString = "";		
		try {
			while (input.hasNextLine()) {         	
        			
				eventString = input.nextLine();
				PositionReport event = PositionReport.parse(eventString);
				if (event.type == 0 && event.dir == 0) output.write(eventString + "\n");            	            	            	         	
			}   
		} catch (IOException e) { System.err.println(e); }	
	}
	
	/**
	 * Copy the given number of tuples from input to output
	 * @param input
	 * @param output
	 * @param tuple number
	 */
	public void copyTuples (Scanner input, BufferedWriter output, int tupleNumber) {
		
		String eventString = "";
		int count = 0; 
		try {
			while (input.hasNextLine() && count < tupleNumber) {         	
        		
				count++;
				eventString = input.nextLine();
				output.write(eventString + "\n");            	            	            	         	
			}   
		} catch (IOException e) { System.err.println(e); }				
	}
	
	/**
	 * Count the number of tuples in the input and print it and the last tuple
	 * @param input 
	 */
	public void countTuples (Scanner input) {
		
		String eventString = "";	
		int count = 0; 
		while (input.hasNextLine()) {         	
        			
			count++;
			eventString = input.nextLine();
		} 
		System.out.println("Count: " + count + " Last event: " + eventString);		
	}
}
