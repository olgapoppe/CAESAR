package iogenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;
import event.*;

/***
 * Input file generator parses the input file and copies certain tuples to the output file.  
 * @author Olga Poppe
 */
public class InputFileGenerator_PA {
	
	/***
	 * Generate input files
	 * @param args: action: 1 for clean file, 
	 * 						2 for merge files, 
	 * 						3 for copy all tuples not later than last second, 
	 * 				path : src/input/ or ../../input/ or ../../../Dropbox/LR/InAndOutput/10xways/
	 * 				if clean file: input file, output file, person id
	 * 				if merge files: first input file, second input file, output file
	 * 				if copy tuples: input file, output file, last second				
	 */
	public static void main (String[] args) {
		
		/*** Validate the input parameter ***/
		if (args.length < 5) {
			System.out.println("At least 5 input parameters are expected.");
			return;
		}	
		
		/*** Instantiate local variables ***/
		int action = Integer.parseInt(args[0]);
		String path = args[1];
				
		/*** Clean file ***/		
		if (action == 1) {
			
			String inputfile = path + args[2];
			String outputfile = path + args[3];
			int pid = Integer.parseInt(args[4]);
			System.out.println("The file " + inputfile + " is cleaned, the person id is set to " + pid + ", the result is saved in the file" + outputfile);
			cleanFile(inputfile,outputfile,pid);			
		} 
		/*** Merge file ***/
		if (action == 2) {
			
			String inputfile1 = path + args[2];
			String inputfile2 = path + args[3];
			String outputfile = path + args[4];
			System.out.println("The files " + inputfile1 + " and " + inputfile2 + " are merged into the file" + outputfile);
			mergeFiles(inputfile1,inputfile2,outputfile);
		}	
		/*** Copy n tuples ***/
		if (action == 3) {
			
			String inputfile = path + args[2];
			String outputfile = path + args[3];
			int n = Integer.parseInt(args[4]);
			System.out.println(n + " events from the file " + inputfile + " are copied to the file" + outputfile);
			getTuples(inputfile,outputfile,n);
		}				
		System.out.println("Done!");
	}
	
	/****************************************************************************
	 * Select correct position reports and change xway
	 * @param inputfilename
	 * @param outputfilename
	 * @param pid person identifier
	 */
	public static void cleanFile (String inputfilename, String outputfilename, int pid) {
		
		Scanner input = null;
		try {		
			/*** Input file ***/
			File input_file = new File(inputfilename);
			input = new Scanner(input_file);  			
								
			/*** Output file ***/
            File output_file = new File(outputfilename);
            BufferedWriter output = new BufferedWriter(new FileWriter(output_file));            
              
            /*** Call method ***/            
            addPid(input,output,pid);
            
            /*** Close the files ***/
       		input.close();
       		output.close();        		
        
		} catch (IOException e) { System.err.println(e); }		  
	}
	
	/***
	 * Copy position reports from input to output and change their xway to the given value 
	 * @param input
	 * @param output
	 * @param pid person identifier
	 */
	public static void addPid (Scanner input, BufferedWriter output, int pid) {
		
		String eventString = "";
		int count = 0; 
		try {
			while (input.hasNextLine()) {         	
        			
				eventString = input.nextLine();
				ActivityReport event = ActivityReport.parse_original_file(eventString);
				
				count++;
				output.write(event.toStringChangePid(pid) + "\n");            	            	            	         	
			}		
		} catch (IOException e) { System.err.println(e); }
		System.out.println("Count: " + count + " Last event: " + eventString);
	}
	
	/****************************************************************************
	 * Merge 2 files
	 * @param filename1
	 * @param filename2
	 */
	public static void mergeFiles (String inputfilename1, String inputfilename2, String outputfilename) {
		
		int lastSec = 4475;
		Scanner input1 = null;
		Scanner input2 = null;
		try {		
			/*** Input file ***/
			File input_file_1 = new File(inputfilename1);
			File input_file_2 = new File(inputfilename2);
			input1 = new Scanner(input_file_1);  			
			input2 = new Scanner(input_file_2);
					
			/*** Output file ***/
            File output_file = new File(outputfilename);
            BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
            
            /*** Call method ***/            
            merge(input1,input2,output,lastSec);
                       
            /*** Close the files ***/
       		input1.close();
       		input2.close();
       		output.close();        		
        
		} catch (IOException e) { System.err.println(e); }		  
	}
	
	/***
	 * Merges 2 sorted files input1 and input2 into one sorted file output. The files are sorted by time stamps. 
	 * @param input1
	 * @param input2
	 * @param output
	 * @param lastSec last second in both input files
	 */
	public static void merge (Scanner input1, Scanner input2, BufferedWriter output, int lastSec) {
		
		String eventString1 = input1.nextLine();
		String eventString2 = input2.nextLine();
		ActivityReport event1 = ActivityReport.parse(eventString1);
		ActivityReport event2 = ActivityReport.parse(eventString2);
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
						event1 = ActivityReport.parse(eventString1);
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
						event2 = ActivityReport.parse(eventString2);
					} else {
						event2 = null;
					}
				} 	
				curr_sec++;
			}
		} catch (IOException e) { System.err.println(e); }	
		System.out.println("Count: " + count + " Last event: " + eventString2);
	}
	
	/****************************************************************************
	 * Copy all tuples with given direction or first n tuples
	 * @param inputfilename
	 * @param outputfilename
	 * @param lastSec
	 */
	public static void getTuples (String inputfilename, String outputfilename, int lastSec) {
		
		Scanner input = null;
		try {		
			/*** Input file ***/
			File input_file = new File(inputfilename);
	        input = new Scanner(input_file);     
	        
	        /*** Output file ***/
            File output_file = new File(outputfilename);
            BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
	            
	        /*** Call method ***/    
            selectTuples(input,output,lastSec);
            
	        /*** Close the files ***/       		
	       	input.close();       		       		
	       	output.close();
	        
		} catch (IOException e) { System.err.println(e); }		  
	}	
	
	/***
	 * Copy all events not later than last second from input to output
	 * @param input
	 * @param output
	 * @param last second
	 */
	public static void selectTuples (Scanner input, BufferedWriter output, int lastSec) {
		
		String eventString = "";
		int count = 0; 
		try {
			while (input.hasNextLine()) {         	
        			
				eventString = input.nextLine();
				ActivityReport event = ActivityReport.parse(eventString);
				
				if (event.sec <= lastSec) {
					
					count++;
					output.write(eventString + "\n");            	            	            	         	
				}
			}   
		} catch (IOException e) { System.err.println(e); }	
		System.out.println("Count: " + count + " Last event: " + eventString);
	}
}
