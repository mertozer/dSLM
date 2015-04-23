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
    
    public void konusmaSayisiAglariniAnalizEt() throws IOException{
    	long startTime = System.currentTimeMillis();
    	
    	konusmaSayisiAglariniOlustur();
    	hashdenIntegeraGec();
		integerDegerleriniMerkezIleEsitle();
    	aglariYonsuzHaleGetir();
		konusmaSayisiAglariUzerindeSLMCalistir();
    	
    	logger.log(Level.FINE, "Bitti Süre: " + ((System.currentTimeMillis() - startTime)/60000.0) + " dk");
    }

	public void konusmaSayisiAglariUzerindeSLMCalistir()
			throws IOException {
		ArrayList<String> inputFileNames = new ArrayList<String>();
		//inputFileNames.add("C:\\SLM\\konusmaSayisi\\toplam\\network.txt");
		inputFileNames.add("C:\\SLM\\konusmaSayisi\\haftaIci\\network.txt");
		inputFileNames.add("C:\\SLM\\konusmaSayisi\\haftaSonu\\network.txt");
		inputFileNames.add("C:\\SLM\\konusmaSayisi\\aksamSaati\\toplam\\network.txt");
		inputFileNames.add("C:\\SLM\\konusmaSayisi\\aksamSaati\\haftaIci\\network.txt");
		inputFileNames.add("C:\\SLM\\konusmaSayisi\\aksamSaati\\haftaSonu\\network.txt");
		inputFileNames.add("C:\\SLM\\konusmaSayisi\\isSaati\\toplam\\network.txt");
		inputFileNames.add("C:\\SLM\\konusmaSayisi\\isSaati\\haftaIci\\network.txt");
		inputFileNames.add("C:\\SLM\\konusmaSayisi\\isSaati\\haftaSonu\\network.txt");
		runSLM(inputFileNames);
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

	public void aglariKenarDegerlerineGoreFiltrele() throws IOException {
		filter("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\toplam\\konusmaSayisiUndirected.txt", 12);
		filter("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\haftaIci\\veriUndirected.txt", 8);
		filter("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\haftaSonu\\veriUndirected.txt", 4);
		filter("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\aksamSaati\\toplam\\veriUndirected.txt", 6);
		filter("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\aksamSaati\\haftaIci\\veriUndirected.txt", 4);
		filter("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\aksamSaati\\haftaSonu\\veriUndirected.txt", 2);
		filter("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\isSaati\\toplam\\veriUndirected.txt", 6);
		filter("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\isSaati\\haftaIci\\veriUndirected.txt", 4);
		filter("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\isSaati\\haftaSonu\\veriUndirected.txt", 2);
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

	public void aglariYonsuzHaleGetir() throws IOException {
		makeGraphUndirected("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\toplam\\konusmaSayisiIntMin1SLM.txt");
		makeGraphUndirected("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\haftaIci\\veriIntMin1SLM.txt");
		makeGraphUndirected("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\haftaSonu\\veriIntMin1SLM.txt");
		makeGraphUndirected("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\aksamSaati\\toplam\\veriIntMin1SLM.txt");
		makeGraphUndirected("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\aksamSaati\\haftaIci\\veriIntMin1SLM.txt");
		makeGraphUndirected("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\aksamSaati\\haftaSonu\\veriIntMin1SLM.txt");
		makeGraphUndirected("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\isSaati\\toplam\\veriIntMin1SLM.txt");
		makeGraphUndirected("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\isSaati\\haftaIci\\veriIntMin1SLM.txt");
		makeGraphUndirected("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\isSaati\\haftaSonu\\veriIntMin1SLM.txt");
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
    			logger.log(Level.FINE, counter + " Süre: " + ((System.currentTimeMillis() - startTime)/1000.0) + " sn");
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
    			logger.log(Level.FINE, counter + " Süre: " + ((System.currentTimeMillis() - startTime)/1000.0) + " sn");
    		}
		}
		
		br2.close();
		writer.close();
		db.close();
    }

	public void integerDegerleriniMerkezIleEsitle() throws IOException {
		convertTotalIntsToSLMInts("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\haftaIci\\veriIntMin1.txt");
		convertTotalIntsToSLMInts("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\haftaSonu\\veriIntMin1.txt");
		convertTotalIntsToSLMInts("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\aksamSaati\\toplam\\veriIntMin1.txt");
		convertTotalIntsToSLMInts("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\aksamSaati\\haftaIci\\veriIntMin1.txt");
		convertTotalIntsToSLMInts("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\aksamSaati\\haftaSonu\\veriIntMin1.txt");
		convertTotalIntsToSLMInts("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\isSaati\\toplam\\veriIntMin1.txt");
		convertTotalIntsToSLMInts("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\isSaati\\haftaIci\\veriIntMin1.txt");
		convertTotalIntsToSLMInts("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\isSaati\\haftaSonu\\veriIntMin1.txt");
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

	public void hashdenIntegeraGec() throws IOException {
		File dbFile = File.createTempFile("mapdb","db");
		DB db = DBMaker.newFileDB(dbFile).closeOnJvmShutdown().deleteFilesAfterClose().transactionDisable().mmapFileEnablePartial().make();
		map = db.createTreeMap("map").makeStringMap();
    	
    	convertHashesToInts("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\toplam\\konusmaSayisi.txt", 1);
		convertHashesToInts("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\haftaIci\\veri.txt", 1);
		convertHashesToInts("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\haftaSonu\\veri.txt", 1);
		convertHashesToInts("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\aksamSaati\\toplam\\veri.txt", 1);
		convertHashesToInts("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\aksamSaati\\haftaIci\\veri.txt", 1);
		convertHashesToInts("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\aksamSaati\\haftaSonu\\veri.txt", 1);
		convertHashesToInts("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\isSaati\\toplam\\veri.txt", 1);
		convertHashesToInts("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\isSaati\\haftaIci\\veri.txt", 1);
		convertHashesToInts("E:\\CallDetailRecordsCommunityAnalysisFiles\\konusmaSayisi\\isSaati\\haftaSonu\\veri.txt", 1);
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
    
    public void printNumberOfLines() throws FileNotFoundException, UnsupportedEncodingException, IOException {
		PrintWriter writer = new PrintWriter("numberOfLines.txt", "UTF-8");
    	
	    writer.println(getNumberOfRecords("H:\\TezDatasi\\Clusters of Data\\konusmaVerisi.txt"));
	    writer.println(getNumberOfRecords("H:\\TezDatasi\\Clusters of Data\\konusmaVerisiAksamSaati.txt"));
	    writer.println(getNumberOfRecords("H:\\TezDatasi\\Clusters of Data\\konusmaVerisiHaftaIci.txt"));
	    writer.println(getNumberOfRecords("H:\\TezDatasi\\Clusters of Data\\konusmaVerisiHaftaIciAksamSaati.txt"));
	    writer.println(getNumberOfRecords("H:\\TezDatasi\\Clusters of Data\\konusmaVerisiHaftaIciIsSaati.txt"));
	    writer.println(getNumberOfRecords("H:\\TezDatasi\\Clusters of Data\\konusmaVerisiHaftaSonu.txt"));
	    writer.println(getNumberOfRecords("H:\\TezDatasi\\Clusters of Data\\konusmaVerisiHaftaSonuAksamSaati.txt"));
	    writer.println(getNumberOfRecords("H:\\TezDatasi\\Clusters of Data\\konusmaVerisiHaftaSonuIsSaati.txt"));
	    writer.println(getNumberOfRecords("H:\\TezDatasi\\Clusters of Data\\konusmaVerisiIsSaati.txt"));
	    
	    writer.println(getNumberOfRecords("H:\\TezDatasi\\Clusters of Data\\smsVerisi.txt"));
	    writer.println(getNumberOfRecords("H:\\TezDatasi\\Clusters of Data\\smsVerisiAksamSaati.txt"));
	    writer.println(getNumberOfRecords("H:\\TezDatasi\\Clusters of Data\\smsVerisiHaftaIci.txt"));
	    writer.println(getNumberOfRecords("H:\\TezDatasi\\Clusters of Data\\smsVerisiHaftaIciAksamSaati.txt"));
	    writer.println(getNumberOfRecords("H:\\TezDatasi\\Clusters of Data\\smsVerisiHaftaIciIsSaati.txt"));
	    writer.println(getNumberOfRecords("H:\\TezDatasi\\Clusters of Data\\smsVerisiHaftaSonu.txt"));
	    writer.println(getNumberOfRecords("H:\\TezDatasi\\Clusters of Data\\smsVerisiHaftaSonuAksamSaati.txt"));
	    writer.println(getNumberOfRecords("H:\\TezDatasi\\Clusters of Data\\smsVerisiHaftaSonuIsSaati.txt"));
	    writer.println(getNumberOfRecords("H:\\TezDatasi\\Clusters of Data\\smsVerisiIsSaati.txt"));
	    
	    writer.println(getNumberOfRecords("H:\\TezDatasi\\Clusters of Data\\gprsVerisi.txt"));
	    
	    writer.println(getNumberOfRecords("E:\\Call Detail Records Community Analysis Files\\konusmaSayisi\\toplam\\konusmaSayisi.txt"));
	    writer.println(getNumberOfRecords("E:\\Call Detail Records Community Analysis Files\\konusmaSayisi\\haftaIci\\veri.txt"));
	    writer.println(getNumberOfRecords("E:\\Call Detail Records Community Analysis Files\\konusmaSayisi\\haftaSonu\\veri.txt"));
	    writer.println(getNumberOfRecords("E:\\Call Detail Records Community Analysis Files\\konusmaSayisi\\aksamSaati\\toplam\\veri.txt"));
	    writer.println(getNumberOfRecords("E:\\Call Detail Records Community Analysis Files\\konusmaSayisi\\aksamSaati\\haftaIci\\veri.txt"));
	    writer.println(getNumberOfRecords("E:\\Call Detail Records Community Analysis Files\\konusmaSayisi\\aksamSaati\\haftaSonu\\veri.txt"));
	    writer.println(getNumberOfRecords("E:\\Call Detail Records Community Analysis Files\\konusmaSayisi\\isSaati\\toplam\\veri.txt"));
	    writer.println(getNumberOfRecords("E:\\Call Detail Records Community Analysis Files\\konusmaSayisi\\isSaati\\haftaIci\\veri.txt"));
	    writer.println(getNumberOfRecords("E:\\Call Detail Records Community Analysis Files\\konusmaSayisi\\isSaati\\haftaSonu\\veri.txt"));
    	writer.close();
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
//        		logger.log(Level.FINE, linenumber + " Süre: " + ((System.currentTimeMillis() - startTime)/1000.0) + " sn");
//    		}	
        }

        logger.log(Level.FINE, source + " lines: " + linenumber);
        lnr.close();
        return source + " lines: " + linenumber;
    }
    
	public void konusmaSayisiAglariniOlustur() throws IOException {
		konusmaSayisi("H:\\TezDatasi\\konusmaVerisi.txt", "H:\\TezDatasi\\konusmaSayisi\\toplam\\konusmaSayisi.txt");
    	konusmaSayisi("C:\\konusmaVerisiHaftaIci.txt", "E:\\CFinder Files\\konusmaSayisi\\haftaIci\\veri.txt");
    	konusmaSayisi("C:\\konusmaVerisiHaftaSonu.txt", "E:\\CFinder Files\\konusmaSayisi\\haftaSonu\\veri.txt");
    	konusmaSayisi("C:\\konusmaVerisiHaftaSonuAksamSaati.txt", "E:\\CFinder Files\\konusmaSayisi\\aksamSaati\\haftaSonu\\veri.txt");
    	konusmaSayisi("C:\\konusmaVerisiHaftaSonuIsSaati.txt", "E:\\CFinder Files\\konusmaSayisi\\isSaati\\haftaSonu\\veri.txt");
    	konusmaSayisi("C:\\konusmaVerisiHaftaIciAksamSaati.txt", "E:\\CFinder Files\\konusmaSayisi\\aksamSaati\\haftaIci\\veri.txt");
    	konusmaSayisi("C:\\konusmaVerisiHaftaIciIsSaati.txt", "E:\\CFinder Files\\konusmaSayisi\\isSaati\\haftaIci\\veri.txt");
    	konusmaSayisi("C:\\konusmaVerisiAksamSaati.txt", "E:\\CFinder Files\\konusmaSayisi\\aksamSaati\\toplam\\veri.txt");
    	konusmaSayisi("C:\\konusmaVerisiIsSaati.txt", "E:\\CFinder Files\\konusmaSayisi\\isSaati\\toplam\\veri.txt");
	}
    
	public void konusmaSayisi(String source, String target) throws IOException{
    	FileInputStream fstream = new FileInputStream(source);
    	DataInputStream in = new DataInputStream(fstream);
    	BufferedReader br = new BufferedReader(new InputStreamReader(in));
    	PrintWriter writer = new PrintWriter(target, "UTF-8");
    	
    	File dbFile = File.createTempFile("mapdb","db");
        DB db = DBMaker.newFileDB(dbFile).closeOnJvmShutdown().deleteFilesAfterClose().transactionDisable().mmapFileEnablePartial().make();
    	
    	//DB db = DBMaker.newMemoryDirectDB().transactionDisable().asyncWriteFlushDelay(100).make();
        
        Map<String,Integer> map = db.createTreeMap("map").makeStringMap();
        
    	String line;
    	String person1;
    	String person2;
    	String cdrtype;
    	int counter=0;
    	long startTime = System.currentTimeMillis();
    	long commitStartTime = System.currentTimeMillis();
    	while ((line = br.readLine()) != null){
    		if(counter%100000==0){
    			startTime = System.currentTimeMillis();
    			if(counter%10000000==0){
    				commitStartTime = System.currentTimeMillis();
    			}
    		}
    		String[] record = line.split(",");
    		person1 = record[1];
    		person2 = record[4];
    		cdrtype = record[8];
    		
    		if(cdrtype.equals("mmt")){
    			String temp=person1;
    			person1=person2;
    			person2=temp;
    		}
    		
    		String newLine = person1 + " " + person2;
    		
    		Integer value = map.get(newLine);
    		if(value!=null){
    			value++;
    		}else{
    			value=1;
    		}
    		map.put(newLine, value);
    		
    		counter++;
    		if(counter%100000==0){
    			logger.log(Level.FINE, counter + " Süre: " + ((System.currentTimeMillis() - startTime)/1000.0) + " sn");
    			if(counter%10000000==0){
        			//db.commit(); //Transaction disabled iken gerek yokmuþ.
        			logger.log(Level.FINE, counter + " : Commit yapýldý. Süre: " + ((System.currentTimeMillis() - commitStartTime)/60000.0) + " dk");
        		}
    		}
    	}
    	
    	int saglama=0;
    	for (Map.Entry<String, Integer> entry : map.entrySet()) {
    		String key = entry.getKey();
    		Integer value = entry.getValue();
    		
    		writer.println(key + " " + value);
    		saglama+=value;
    	}
    	logger.log(Level.FINE, "saðlama toplam record sayýsý : " + saglama);
    	
    	in.close();
    	writer.close();
    	db.close();
    }
	
    public void konusmaSuresi(String source, String target) throws IOException{
    	FileInputStream fstream = new FileInputStream(source);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		PrintWriter writer = new PrintWriter(target, "UTF-8");
		
		Map<String, Integer> map = new HashMap<String, Integer>();
		
		String line;
  		String person1;
  		String person2;
  		String cdrtype;
  		String duration;
  		while ((line = br.readLine()) != null){
	  		String[] record = line.split(",");
	  		person1 = record[1];
	  		person2 = record[4];
	  		cdrtype = record[8];
	  		duration = record[10];
	  		
	  		if(cdrtype.equals("mmt")){
	  			String temp=person1;
	  			person1=person2;
	  			person2=temp;
	  		}
	  		
	  		String newLine = person1 + " " + person2;
	  		
	  		Integer value = map.get(newLine);
    		if(value!=null){
    			value=value+Integer.parseInt(duration);
    		}else{
    			value=Integer.parseInt(duration);
    		}
    		map.put(newLine, value);
	  	}
  		
    	for (Map.Entry<String, Integer> entry : map.entrySet()) {
    	    String key = entry.getKey();
    	    Integer value = entry.getValue();
    	    
    	    writer.println(key + " " + value);
    	}

		in.close();
		writer.close();
    }

    public void smsSayisi(String source, String target) throws IOException{
    	FileInputStream fstream = new FileInputStream(source);
    	DataInputStream in = new DataInputStream(fstream);
    	BufferedReader br = new BufferedReader(new InputStreamReader(in));
    	PrintWriter writer = new PrintWriter(target, "UTF-8");
    	
    	Map<String, Integer> map = new HashMap<String, Integer>();
    	
    	String line;
    	String person1;
    	String person2;
    	String cdrtype;
    	while ((line = br.readLine()) != null){
    		String[] record = line.split(",");
    		person1 = record[1];
    		person2 = record[4];
    		cdrtype = record[8];
    		
    		if(cdrtype.equals("msmt")){
    			String temp=person1;
    			person1=person2;
    			person2=temp;
    		}
    		
    		String newLine = person1 + " " + person2;
    		
    		Integer value = map.get(newLine);
    		if(value!=null){
    			value++;
    		}else{
    			value=1;
    		}
    		map.put(newLine, value);
    	}
    	
    	int saglama=0;
    	for (Map.Entry<String, Integer> entry : map.entrySet()) {
    		String key = entry.getKey();
    		Integer value = entry.getValue();
    		
    		writer.println(key + " " + value);
    		saglama+=value;
    	}
    	logger.log(Level.FINE, ""+saglama);
    	
    	in.close();
    	writer.close();
    }
	
    public void veriyiAyristir() throws IOException {
		veriExtractFull();
    	haftaVerisiExtract("H:\\TezDatasi\\konusmaVerisi.txt");
    	saatVerisiExtract("H:\\TezDatasi\\konusmaVerisi.txt");
    	saatVerisiExtract("H:\\TezDatasi\\konusmaVerisiHaftaIci.txt");
    	saatVerisiExtract("H:\\TezDatasi\\konusmaVerisiHaftaSonu.txt");
    	haftaVerisiExtract("H:\\TezDatasi\\smsVerisi.txt");
    	saatVerisiExtract("H:\\TezDatasi\\smsVerisi.txt");
    	saatVerisiExtract("H:\\TezDatasi\\smsVerisiHaftaIci.txt");
    	saatVerisiExtract("H:\\TezDatasi\\smsVerisiHaftaSonu.txt");
	}
    
    public void veriExtractFull() throws IOException{
    	PrintWriter writerKonusma = new PrintWriter("H:\\TezDatasi\\konusmaVerisi.txt", "UTF-8");
    	PrintWriter writerSMS = new PrintWriter("H:\\TezDatasi\\smsVerisi.txt", "UTF-8");
    	PrintWriter writerGPRS = new PrintWriter("H:\\TezDatasi\\gprsVerisi.txt", "UTF-8");
    	
    	File parcaliVeriKlasoru = new File("H:\\TezDatasi\\splittedParts");
    	File[] parcaliVeriler = parcaliVeriKlasoru.listFiles();
    	for(File parcaliVeri : parcaliVeriler){
    		long startTime = System.currentTimeMillis();
        	
    		FileInputStream fstream = new FileInputStream(parcaliVeri);
        	DataInputStream in = new DataInputStream(fstream);
        	BufferedReader br = new BufferedReader(new InputStreamReader(in));
        	String line;
        	while ((line = br.readLine()) != null){
        		if(line.contains("mmo") || line.contains("mmt")){
        			writerKonusma.println(line);
        		}else if(line.contains("msmo") || line.contains("msmt")){
        			writerSMS.println(line);
        		}else if(line.contains("gprs")){
        			writerGPRS.println(line);
        		}else{
        			logger.log(Level.FINE, "---------------" + line + "******************");
        		}
        	}
        	in.close();

        	logger.log(Level.FINE, parcaliVeri.getName() + " Süre: " + ((System.currentTimeMillis() - startTime)/1000.0) + " sn");
    	}
    	
    	writerKonusma.close();
    	writerSMS.close();
    	writerGPRS.close();
    }
    
    public void haftaVerisiExtract(String source) throws IOException{
    	long startTime = System.currentTimeMillis();
    	FileInputStream fstream = new FileInputStream(source);
    	DataInputStream in = new DataInputStream(fstream);
    	BufferedReader br = new BufferedReader(new InputStreamReader(in));
    	PrintWriter writerHaftaIci = new PrintWriter(source.subSequence(0, source.length()-4)+"HaftaIci.txt", "UTF-8");
    	PrintWriter writerHaftaSonu = new PrintWriter(source.subSequence(0, source.length()-4)+"HaftaSonu.txt", "UTF-8");
    	String line;
    	while ((line = br.readLine()) != null){
    		if(line.contains("20120903") || line.contains("20120904") || line.contains("20120905") || line.contains("20120906") || line.contains("20120907") ||
    				line.contains("20120910") || line.contains("20120911") || line.contains("20120912") || line.contains("20120913") || line.contains("20120914") || 
    				line.contains("20120917") || line.contains("20120918") || line.contains("20120919") || line.contains("20120920") || line.contains("20120921") ||
    				line.contains("20120924") || line.contains("20120925") || line.contains("20120926") || line.contains("20120927") || line.contains("20120928")){
    			writerHaftaIci.println(line);
    		}else{
    			writerHaftaSonu.println(line);
    		}
    	}
    	in.close();
    	writerHaftaIci.close();
    	writerHaftaSonu.close();
    	logger.log(Level.FINE, source + " hafta verisi oluþturuldu. Süre: " + ((System.currentTimeMillis() - startTime)/60000.0) + " dk");
    }
    
    public void saatVerisiExtract(String source) throws IOException{
    	long startTime = System.currentTimeMillis();
    	FileInputStream fstream = new FileInputStream(source);
    	DataInputStream in = new DataInputStream(fstream);
    	BufferedReader br = new BufferedReader(new InputStreamReader(in));
    	PrintWriter writerIsSaati = new PrintWriter(source.subSequence(0, source.length()-4)+"IsSaati.txt", "UTF-8");
    	PrintWriter writerAksamSaati = new PrintWriter(source.subSequence(0, source.length()-4)+"AksamSaati.txt", "UTF-8");
    	String line;
    	while ((line = br.readLine()) != null){
    		String[] record = line.split(",");
    		int saat = Integer.parseInt(record[7]);
    		if(saat > 80000 && saat < 170000){
    			writerIsSaati.println(line);
    		}else{
    			writerAksamSaati.println(line);
    		}
    	}
    	in.close();
    	writerIsSaati.close();
    	writerAksamSaati.close();
    	logger.log(Level.FINE, source + " saat verisi oluþturuldu. Süre: " + ((System.currentTimeMillis() - startTime)/60000.0) + " dk");
    }
}


