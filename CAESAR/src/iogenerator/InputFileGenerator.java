package iogenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

/**
 * Input file generator parses the input file and
 * copies a given number of lines to the output file. 
 * 
 * @author olga
 */
public class InputFileGenerator {
	
	public static void main (String[] args) {
	
		Scanner s = null;
		try {		
			/*** Input file ***/
			//File f = new File("../../input_till_sec_10784.dat");
			File f = new File("../../../Dropbox/LR/InAndOutput/1xway/input7.dat");
			s = new Scanner(f);  			
					
			/*** Output file ***/
            File input_file = new File("../../input.dat");
            BufferedWriter position_reports = new BufferedWriter(new FileWriter(input_file));
            
            String eventString = "";
            int count = 0;
            int number_of_position_reports = 357853; // Integer.parseInt(args[0]);
            
            /*** Write output file ***/
            while (s.hasNextLine() && count<number_of_position_reports) {        	
            	count++;
            	eventString = s.nextLine();
            	position_reports.write(eventString + "\n");            	            	            	         	
            } 
            // Print out event number and last event
            System.out.println("Count: " + count + " Last event: " + eventString);
            
            /*** Clean-up ***/
       		s.close();
       		position_reports.close();        		
        
		} catch (IOException e) { System.err.println(e); }		  
	}
}
