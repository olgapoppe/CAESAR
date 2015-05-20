package iogenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;
import event.*;

/**
 * Input file generator parses the input file and copies certain tuples to the output file.  
 * @author Olga Poppe
 */
public class InputFileGenerator {
	
	// select correct position reports and change xway
	public static void main1 (String[] args) {
		
		Scanner input = null;
		try {		
			/*** Input file ***/
			File input_file = new File("../../Dropbox/LR/InAndOutput/6xways/cardatapoints.out5");
			input = new Scanner(input_file);  			
								
			/*** Output file ***/
            File output_file = new File("../../input5.dat");
            BufferedWriter output = new BufferedWriter(new FileWriter(output_file));            
              
            /*** Call method ***/            
            changeXway(input,output,5);
            
            /*** Close the files ***/
       		input.close();
       		output.close();        		
        
		} catch (IOException e) { System.err.println(e); }		  
	}
	
	// merge 2 files
	public static void main2 (String[] args) {
		
		int lastSec = 10784;
		Scanner input1 = null;
		Scanner input2 = null;
		try {		
			/*** Input file ***/
			File input_file_1 = new File("../../Dropbox/LR/InAndOutput/6xways/merged0123.dat");
			File input_file_2 = new File("../../Dropbox/LR/InAndOutput/6xways/merged45.dat");
			input1 = new Scanner(input_file_1);  			
			input2 = new Scanner(input_file_2);
					
			/*** Output file ***/
            File output_file = new File("../../merged012345.dat");
            BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
            
            /*** Call method ***/            
            merge(input1,input2,output,lastSec);
                       
            /*** Close the files ***/
       		input1.close();
       		input2.close();
       		output.close();        		
        
		} catch (IOException e) { System.err.println(e); }		  
	}
	
	// count number of tuples in the merged file
	public static void main3 (String[] args) {
	
		Scanner input = null;
		try {		
			/*** Input file ***/
			File input_file = new File("../../Dropbox/LR/InAndOutput/3xways/input2.dat");
            input = new Scanner(input_file);           
            
            /*** Call method ***/                      
            countTuples(input);
            
            /*** Close the files ***/       		
       		input.close();       		       		
        
		} catch (IOException e) { System.err.println(e); }		  
	}
	
	/**
	 * Merges 2 sorted files input1 and input2 into one sorted file output. The files are sorted by time stamps. 
	 * @param input1
	 * @param input2
	 * @param output
	 * @param lastSec last second in both input files
	 */
	public static void merge (Scanner input1, Scanner input2, BufferedWriter output, int lastSec) {
		
		String eventString1 = input1.nextLine();
		String eventString2 = input2.nextLine();
		PositionReport event1 = PositionReport.parse(eventString1);
		PositionReport event2 = PositionReport.parse(eventString2);
		double curr_sec = 0;
		int count = 0; 
		
		try {
			
			while (curr_sec <= lastSec) {							
				
				while (event1 != null && event1.sec == curr_sec) {
					
					count++;
						
					// Write event1
					output.write(eventString1 + "\n");
						
					// Reset event1
					if (input1.hasNextLine()) {
						eventString1 = input1.nextLine();
						event1 = PositionReport.parse(eventString1);
					} else {
						event1 = null;
					}
				} 		
				while (event2 != null && event2.sec == curr_sec) {
					
					count++;
					
					// Write event2
					output.write(eventString2 + "\n");
					
					// Reset event2
					if (input2.hasNextLine()) {
						eventString2 = input2.nextLine();
						event2 = PositionReport.parse(eventString2);
					} else {
						event2 = null;
					}
				} 	
				curr_sec++;
			}
		} catch (IOException e) { System.err.println(e); }	
		System.out.println("Count: " + count + " Last event: " + eventString2);
	}
	
	/**
	 * Copy position reports from input to output and change their xway to the given value 
	 * @param input
	 * @param output
	 * @param new xway
	 */
	public static void changeXway (Scanner input, BufferedWriter output, int newXway) {
		
		String eventString = "";
		int count = 0; 
		try {
			while (input.hasNextLine()) {         	
        			
				eventString = input.nextLine();
				PositionReport event = PositionReport.parse(eventString);
				
				if (event.correctPositionReport()) {
					
					count++;
					output.write(event.toString(newXway) + "\n");            	            	            	         	
			}}			
		} catch (IOException e) { System.err.println(e); }
		System.out.println("Count: " + count + " Last event: " + eventString);
	}
	
	/**
	 * Copy position reports that have the given direction from input to output
	 * @param input
	 * @param output
	 * @param dir
	 */
	public static void selectTuples (Scanner input, BufferedWriter output, int dir) {
		
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
	public static void copyTuples (Scanner input, BufferedWriter output, int tupleNumber) {
		
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
	public static void countTuples (Scanner input) {
		
		String eventString = "";	
		int count = 0; 
		while (input.hasNextLine()) {         	
        			
			count++;
			eventString = input.nextLine();
		} 
		System.out.println("Count: " + count + " Last event: " + eventString);		
	}
}
