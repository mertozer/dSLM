package preprocess;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.commons.collections4.map.MultiValueMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import core.ModularityOptimizer;

public class InputNetworkPreprocess{	
	private Map<String, Integer> map;
	private Logger logger = Logger.getLogger( InputNetworkPreprocess.class.getName() );
	private FileHandler fh = null;
	
	public InputNetworkPreprocess() {
		try {
			 fh=new FileHandler("system.log", true);
		} catch (SecurityException | IOException e) {
			 e.printStackTrace();
		}
		fh.setFormatter(new SimpleFormatter());
		logger.addHandler(fh);
		logger.setLevel(Level.FINE);
		
		System.out.println("*********************************************************************************");
		System.out.println("Please check the system.log file in order to see the detailed log information!");
		System.out.println("*********************************************************************************");
	}
	
	public void constructPartOf(String source, int lineBegin, int lineEnd) throws IOException{
    	FileInputStream fstream = new FileInputStream(source);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String sourceFileName = source.substring(0, source.length()-4) + lineEnd;
		PrintWriter writer = new PrintWriter(sourceFileName + ".txt", "UTF-8");
		
		int end = lineEnd;
		
		while (lineBegin>0){
			br.readLine();
			lineBegin--;
			end--;
		}
		
		while (end>0){
			writer.println(br.readLine());
			end--;
		}
		
		br.close();
		writer.close();
		
		convertTotalIntsToSLMInts(sourceFileName + ".txt");
		slmConverter(sourceFileName + "SLM.txt");
		
		System.out.println();
		System.out.println("*********************************************************************************");
		System.out.println(sourceFileName + "SLMSorted.txt is created.");
		System.out.println("*********************************************************************************");
    }
    
    
	
    public void runSLM(ArrayList<String> inputFileNames) throws IOException{
    	ModularityOptimizer slm = new ModularityOptimizer();
    	for(String inputFileName : inputFileNames){
    		logger.log(Level.FINE, "SLM processing for " + inputFileName);
    		slm.detectCommunities(inputFileName, inputFileName.substring(0, inputFileName.lastIndexOf("\\")) + "\\communitiesArray.txt", 1, 0.1, 3, 1, 2, 0L, true, false, null, 1);
    		slmCommunityConverter(inputFileName.substring(0, inputFileName.lastIndexOf("\\")) + "\\communitiesArray.txt");
    	}
    }
    
    @SuppressWarnings("rawtypes")
	public void slmCommunityConverter(String source) throws IOException{
    	FileInputStream fstream = new FileInputStream(source);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		PrintWriter writer = new PrintWriter(source.substring(0, source.length()-20) + "communities" + ".txt", "UTF-8");
		
		Map<String, Integer> map = new HashMap<String, Integer>();
		MultiValueMap<String, Object> relations = new MultiValueMap<String, Object>();
		Map<Integer, Integer> nodeMap = new HashMap<Integer, Integer>();
		
		if(source.contains("haftaIci") || source.contains("haftaSonu") || source.contains("aksamSaati") || source.contains("isSaati")){
			FileInputStream fstreamMatch = new FileInputStream(source.substring(0, source.lastIndexOf("\\")) + "\\match.txt");
			DataInputStream inMatch = new DataInputStream(fstreamMatch);
			BufferedReader brMatch = new BufferedReader(new InputStreamReader(inMatch));
			
			String lineMatch;
			
			while ((lineMatch = brMatch.readLine()) != null){
				String[] tokens = lineMatch.split(" ");
				Integer key = Integer.parseInt(tokens[0]);
				Integer value = Integer.parseInt(tokens[1]);
				if(!nodeMap.containsKey(key)){
					nodeMap.put(key,value);
				}
			}
			
			brMatch.close();
		}
		
		String line;
		int lineNumber = 0;
		while ((line = br.readLine()) != null){
			if(map.containsKey(line)){
				map.put(line, map.get(line)+1);
			}else{
				map.put(line, 1);
			}
			if(source.contains("haftaIci") || source.contains("haftaSonu") || source.contains("aksamSaati") || source.contains("isSaati")){
				relations.put(line, nodeMap.get(lineNumber));
			}else{
				relations.put(line, lineNumber);
			}
			lineNumber++;
		}
		
		List<Entry<String, Integer>> entries = new ArrayList<Entry<String, Integer>>(map.entrySet());
		Collections.sort(entries, new Comparator<Entry<String, Integer>>() {
		    public int compare(Entry<String, Integer> e1, Entry<String, Integer> e2) {
		        return -1 * e1.getValue().compareTo(e2.getValue());
		    }
		});
		
		for(Entry<String, Integer> entry : entries){
			StringBuilder sb = new StringBuilder("Community ID: " + entry.getKey() + " Community Size: " + entry.getValue());
			sb.append(" Nodes: ");
			List nodes = (List) relations.get(entry.getKey());
			for(Object node : nodes){
				sb.append(node);
				sb.append(", ");
			}
			
			writer.println(sb.toString());
		}
		
		br.close();
		writer.close();
    }
    
	@SuppressWarnings("unchecked")
	public void slmConverter(String source) throws IOException{
    	FileInputStream fstream = new FileInputStream(source);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		PrintWriter writer = new PrintWriter(source.substring(0, source.length()-4) + "Sorted" + ".txt", "UTF-8");
		
		MultiValueMap<Integer, Object> map = new MultiValueMap<Integer, Object>();
		
		String line;
		while ((line = br.readLine()) != null){
			String[] tokens = line.split("\t");
			map.put(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]));
		}
		
		List<Entry<Integer, Object>> entries = new ArrayList<Entry<Integer, Object>>(map.entrySet());
		Collections.sort(entries, new Comparator<Entry<Integer, Object>>() {
		    public int compare(Entry<Integer, Object> e1, Entry<Integer, Object> e2) {
		        return e1.getKey()-e2.getKey();
		    }
		});
		
       for(Entry<Integer, Object> entry : entries){
            @SuppressWarnings("rawtypes")
			List list = (List) map.get(entry.getKey());
            Collections.sort(list);
            for (int j = 0; j < list.size(); j++) {
            	writer.println(entry.getKey() + "\t" + list.get(j));
            }
        }
		
		br.close();
		writer.close();
    }
    
    public void slmConverterWithWeights(String source) throws IOException{
    	FileInputStream fstream = new FileInputStream(source);
    	DataInputStream in = new DataInputStream(fstream);
    	BufferedReader br = new BufferedReader(new InputStreamReader(in));
    	PrintWriter writer = new PrintWriter(source.substring(0, source.length()-4) + "Sorted" + ".txt", "UTF-8");
    	
    	TreeMap<Integer, TreeMap<Integer, Integer>> map = new TreeMap<Integer, TreeMap<Integer, Integer>>();
    	
    	String line;
    	while ((line = br.readLine()) != null){
    		String[] tokens = line.split("\t");
    		Integer key = Integer.parseInt(tokens[0]);
    		if(map.containsKey(key)){
    			map.get(key).put(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]));
    		}else{
    			TreeMap<Integer, Integer> innerMap = new TreeMap<Integer, Integer>();
    			innerMap.put(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]));
    			map.put(key, innerMap);
    		}
    	}
    	
    	for(Entry<Integer, TreeMap<Integer, Integer>> entry : map.entrySet()){
    		TreeMap<Integer, Integer> innerMap = entry.getValue();
    		for (Entry<Integer, Integer> innerEntry : innerMap.entrySet()) {
    			writer.println(entry.getKey() + "\t" + innerEntry.getKey() + "\t" + innerEntry.getValue());
    		}
    	}
    	
    	br.close();
    	writer.close();
    }

	
	
    public void filter(String source, int minWeight) throws IOException{
    	FileInputStream fstream = new FileInputStream(source);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		PrintWriter writer = new PrintWriter(source.substring(0, source.length()-4)+"Min" + minWeight + ".txt", "UTF-8");
		
		String line;
		while ((line = br.readLine()) != null){
			String[] tokens = line.split(" ");
			if(Integer.parseInt(tokens[2]) >= minWeight){
				writer.println(line);
			}
		}
		
		br.close();
		writer.close();
    }

	
	
    public void makeGraphUndirected(String source) throws IOException{
    	FileInputStream fstream = new FileInputStream(source);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		PrintWriter writer = new PrintWriter(source.replace("E:\\Call Detail Records Community Analysis Files", "C:\\SLM").substring(0, source.length()-14)+"Undirected.txt", "UTF-8");
		
		File dbFile = File.createTempFile("mapdb","db");
        DB db = DBMaker.newFileDB(dbFile).closeOnJvmShutdown().deleteFilesAfterClose().transactionDisable().mmapFileEnablePartial().make();
		
		Map<String, Integer> map = db.createTreeMap("map").makeStringMap();
		String line;
		int counter = 0;
		long startTime = System.currentTimeMillis(); 
		
		logger.log(Level.FINE, source + " graph undirect part 1");
		
		while ((line = br.readLine()) != null){
			if(counter%1000000==0){
    			startTime = System.currentTimeMillis();
    		}
			String[] tokens = line.split(" ");
			String key = tokens[0] + "\t" + tokens[1];
			if(!map.containsKey(key)){
				map.put(key, Integer.parseInt(tokens[2]));
			}
			
			counter++;
			
			if(counter%1000000==0){
    			logger.log(Level.FINE, counter + " S�re: " + ((System.currentTimeMillis() - startTime)/1000.0) + " sn");
    		}
		}
		
		br.close();
		
		counter=0;
		FileInputStream fstream2 = new FileInputStream(source);
		DataInputStream in2 = new DataInputStream(fstream2);
		BufferedReader br2 = new BufferedReader(new InputStreamReader(in2));
		
		logger.log(Level.FINE, source + " graph undirect part 2");
		while ((line = br2.readLine()) != null){
			if(counter%1000000==0){
    			startTime = System.currentTimeMillis();
    		}
			String[] tokens = line.split(" ");
			String key = tokens[0] + "\t" + tokens[1];
			String reverse = tokens[1] + "\t" + tokens[0];
			if(map.containsKey(key)){
				if(!map.containsKey(reverse)){
					writer.println(line.replace(" ", "\t"));
				}else{
					Integer value = map.get(reverse) + Integer.parseInt(tokens[2]);
					writer.println(key + "\t" + value);
					map.remove(reverse);
				}
				map.remove(key);
			}
			
			counter++; 
			
			if(counter%1000000==0){
    			logger.log(Level.FINE, counter + " S�re: " + ((System.currentTimeMillis() - startTime)/1000.0) + " sn");
    		}
		}
		
		br2.close();
		writer.close();
		db.close();
    }

	
    
    public void convertTotalIntsToSLMInts(String source) throws IOException{
    	File dbFile = File.createTempFile("mapdb","db");
		DB db = DBMaker.newFileDB(dbFile).closeOnJvmShutdown().deleteFilesAfterClose().transactionDisable().mmapFileEnablePartial().make();
		Map<String, Integer> map = db.createTreeMap("map").makeStringMap();    	
    	
    	FileInputStream fstream = new FileInputStream(source);
    	DataInputStream in = new DataInputStream(fstream);
    	BufferedReader br = new BufferedReader(new InputStreamReader(in));
    	PrintWriter writer = new PrintWriter(source.substring(0, source.length()-4)+"SLM.txt", "UTF-8");
    	PrintWriter writerMatch = new PrintWriter(source.substring(0, source.length()-4)+"SLMMatch.txt", "UTF-8");
    	
    	String line;
    	int number=0;
    	while ((line = br.readLine()) != null){
    		String[] tokens = line.split("\t");
    		
    		if(!map.containsKey(tokens[0])){
    			map.put(tokens[0], number);
    			writerMatch.println(number + " " + tokens[0]);
    			number++;
    		}
    		
    		if(!map.containsKey(tokens[1])){
    			map.put(tokens[1], number);
    			writerMatch.println(number + " " + tokens[1]);
    			number++;
    		}
    		
    		if(tokens.length==3){
    			writer.println(map.get(tokens[0]) + "\t" + map.get(tokens[1]) + "\t" + tokens[2]);
    		}else{
    			writer.println(map.get(tokens[0]) + "\t" + map.get(tokens[1]));
    		}
    		
    	}
    	
    	logger.log(Level.FINE, source.substring(0, source.length()-4) + " Map(Node) size: " + map.size());
    	
    	br.close();
    	writer.close();
    	writerMatch.close();
    }

	
	
	public void convertHashesToInts(String source, int minWeight) throws IOException{
	    	FileInputStream fstream = new FileInputStream(source);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			PrintWriter writer = new PrintWriter(source.substring(0, source.length()-4)+"IntMin" + minWeight + ".txt", "UTF-8");
			PrintWriter writerMatch = new PrintWriter(source.substring(0, source.length()-4)+"IntMin" + minWeight + "Match.txt", "UTF-8");
			
			String line;
			int number=0;
			while ((line = br.readLine()) != null){
				String[] tokens = line.split(" ");
				
				if(Integer.parseInt(tokens[2]) < minWeight){
					continue;
				}
				
				if(!map.containsKey(tokens[0])){
					map.put(tokens[0], number);
					writerMatch.println(number + " " + tokens[0]);
					number++;
				}
				
				if(!map.containsKey(tokens[1])){
					map.put(tokens[1], number);
					writerMatch.println(number + " " + tokens[1]);
					number++;
				}
				
				writer.println(map.get(tokens[0]) + " " + map.get(tokens[1]) + " " + tokens[2]);
			}
			
			logger.log(Level.FINE, "Map(Node) size: " + map.size());
			
			br.close();
			writer.close();
			writerMatch.close();
	    }
    
    
    
    public String getNumberOfRecords(String source) throws IOException{
    	FileReader fr = new FileReader(new File(source));
	    LineNumberReader lnr = new LineNumberReader(fr);

	    int linenumber = 0;
//	    long startTime = 0;
        while (lnr.readLine() != null){
//        	if(linenumber%1000000==0){
//        		startTime = System.currentTimeMillis();
//    		}	
        	linenumber++;
//        	if(linenumber%1000000==0){
//        		logger.log(Level.FINE, linenumber + " S�re: " + ((System.currentTimeMillis() - startTime)/1000.0) + " sn");
//    		}	
        }

        logger.log(Level.FINE, source + " lines: " + linenumber);
        lnr.close();
        return source + " lines: " + linenumber;
    }
    
	
}


