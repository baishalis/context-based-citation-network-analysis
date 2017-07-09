package mcad;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class MetadataFiltering {
	
	public static void main(String args[]) throws IOException {
		BufferedReader readCitation = new BufferedReader(new FileReader("/home/iiita/Desktop/citation-context-analysis-master/scraper/metadata.txt"));
		FileWriter fw = new FileWriter("/home/iiita/Desktop/citation-context-analysis-master/scraper/filteredMetadata.txt");
		
		String str = "";
		//String line = readCitation.readLine();

		String read_str = "";
		//int line = 1;
		while((str = readCitation.readLine()) != null) {
			String[] cols = str.split("\\*\\*\\*");
			String[] cols1 = read_str.split("\\*\\*\\*");
			
			if(cols.length == 5 || cols1.length == 5) {
				//straight away write in file 
				if(cols1.length == 5) {
					read_str += "\n";
					fw.write(read_str);
				//	System.out.println(read_str);
				}
				if(cols.length == 5) {
					str += "\n";
					fw.write(str);
				}
				read_str = "";
			}
			else {
				read_str += str;
			}
			
		}
		
		fw.close();
		
	}

}
