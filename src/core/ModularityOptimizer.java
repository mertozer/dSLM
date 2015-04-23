package core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class ModularityOptimizer
{
	private Logger logger = Logger.getLogger( ModularityOptimizer.class.getName() );
	private FileHandler fh = null;
	
	public ModularityOptimizer() {
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
	
    public void detectCommunities(String inputFileName, String outputFileName, Integer modularityFunction,
    								Double resolution, Integer algorithm, Integer nRandomStarts, Integer nIterations, 
    								Long randomSeed, boolean printOutput, boolean dynamic, String clustersFileName, double expectedModularity) throws IOException
    {
    	if(dynamic){
    		outputFileName=outputFileName.replace(".txt", "-dynamic.txt");
    	}else{
    		outputFileName=outputFileName.replace(".txt", "-static.txt");
    	}
    	
        boolean update;
        double modularity, maxModularity, resolution2;
        int i, j, nClusters;
        int[] cluster;
        long beginTime, endTime;
        Network network;
        Random random;
    
        if (printOutput)
        {
            logger.log(Level.FINE, "Reading input file...");
        }

        network = readInputFile(inputFileName, modularityFunction);

        if (printOutput)
        {
            logger.log(Level.FINE, String.format("Number of nodes: %d%n", network.getNNodes()));
            //logger.log(Level.FINE, String.format("Number of edges: %d%n", network.getNEdges() / 2));
            
            logger.log(Level.FINE, "Running " + ((algorithm == 1) ? "Louvain algorithm" : ((algorithm == 2) ? "Louvain algorithm with multilevel refinement" : "smart local moving algorithm")) + "...");
            
        }

        resolution2 = ((modularityFunction == 1) ? (resolution / network.getTotalEdgeWeight()) : resolution);

        beginTime = System.currentTimeMillis();
        cluster = null;
        nClusters = -1;
        maxModularity = Double.NEGATIVE_INFINITY;
        random = new Random(randomSeed);
        for (i = 0; i < nRandomStarts; i++)
        {
            if (printOutput && (nRandomStarts > 1))
                logger.log(Level.FINE, String.format("Random start: %d%n", i + 1));

            if(dynamic){
            	network.initClusters(clustersFileName);
            	modularity = network.calcQualityFunction(resolution2);
            }else{            	
            	network.initSingletonClusters();
            }

            j = 0;
            update = true;
            do
            {
                if (printOutput && (nIterations > 1))
                    logger.log(Level.FINE, String.format("Iteration: %d%n", j + 1));

                if (algorithm == 1)
                    update = network.runLouvainAlgorithm(resolution2, random);
                else if (algorithm == 2)
                    update = network.runLouvainAlgorithmWithMultilevelRefinement(resolution2, random);
                else if (algorithm == 3)
                    network.runSmartLocalMovingAlgorithm(resolution2, random);
                j++;

                modularity = network.calcQualityFunction(resolution2);

                if (printOutput && (nIterations > 1))
                    logger.log(Level.FINE, String.format("Modularity: %.4f%n", modularity));
            }
            while ((j < nIterations) && update && ((double)Math.round(modularity * 10000) / 10000) < expectedModularity);

            if (modularity > maxModularity)
            {
                network.orderClustersByNNodes();
                cluster = network.getClusters();
                nClusters = network.getNClusters();
                maxModularity = modularity;
            }
        }
        endTime = System.currentTimeMillis();

        if (printOutput)
        {
            if (nRandomStarts == 1){
                logger.log(Level.FINE, String.format("Modularity: %.4f%n", maxModularity));
            }else{            	
            	logger.log(Level.FINE, String.format("Maximum modularity in %d random starts: %.4f%n", nRandomStarts, maxModularity));
            }
            
            logger.log(Level.FINE, String.format("Number of communities: %d%n", nClusters));
            
            long milliseconds = endTime - beginTime;
            long seconds = milliseconds / 1000;
            long minutes = seconds / 60;
            milliseconds = milliseconds%1000;
            seconds = seconds%60;
            
            logger.log(Level.FINE, String.format("Elapsed time: %d minutes, %d seconds, and %d milliseconds %n", minutes, seconds, milliseconds));
            
            logger.log(Level.FINE, "Writing output file... Output: " + outputFileName);
            
            System.out.println();
            System.out.println("*********************************************************************************");
            System.out.println((dynamic ? "dSLM" : "SLM") + " run on network " + inputFileName);
            System.out.println(String.format("Modularity: %.4f", maxModularity));
            System.out.println(String.format("Running time: %d minutes, %d seconds, and %d milliseconds", minutes, seconds, milliseconds));
            System.out.println("*********************************************************************************");
        }

        writeOutputFile(outputFileName, cluster);
    }

    private static Network readInputFile(String fileName, int modularityFunction) throws IOException
    {
        BufferedReader bufferedReader;
        double[] edgeWeight1, edgeWeight2, nodeWeight;
        int i, j, nEdges, nLines, nNodes;
        int[] firstNeighborIndex, neighbor, nNeighbors, node1, node2;
        Network network;
        String[] splittedLine;

        bufferedReader = new BufferedReader(new FileReader(fileName));

        nLines = 0;
        while (bufferedReader.readLine() != null)
            nLines++;

        bufferedReader.close();

        bufferedReader = new BufferedReader(new FileReader(fileName));

        node1 = new int[nLines];
        node2 = new int[nLines];
        edgeWeight1 = new double[nLines];
        i = -1;
        for (j = 0; j < nLines; j++)
        {
            splittedLine = bufferedReader.readLine().split("\t");
            node1[j] = Integer.parseInt(splittedLine[0]);
            if (node1[j] > i)
                i = node1[j];
            node2[j] = Integer.parseInt(splittedLine[1]);
            if (node2[j] > i)
                i = node2[j];
            edgeWeight1[j] = (splittedLine.length > 2) ? Double.parseDouble(splittedLine[2]) : 1;
        }
        nNodes = i + 1;
        //TODO: number of nodes hesaplamasý aradaki assign edilmemiþ int deðerlerini de sayýyor, düzelt

        bufferedReader.close();

        nNeighbors = new int[nNodes];
        for (i = 0; i < nLines; i++)
            if (node1[i] < node2[i])
            {
                nNeighbors[node1[i]]++;
                nNeighbors[node2[i]]++;
            }

        firstNeighborIndex = new int[nNodes + 1];
        nEdges = 0;
        for (i = 0; i < nNodes; i++)
        {
            firstNeighborIndex[i] = nEdges;
            nEdges += nNeighbors[i];
        }
        firstNeighborIndex[nNodes] = nEdges;

        neighbor = new int[nEdges];
        edgeWeight2 = new double[nEdges];
        Arrays.fill(nNeighbors, 0);
        for (i = 0; i < nLines; i++)
            if (node1[i] < node2[i])
            {
                j = firstNeighborIndex[node1[i]] + nNeighbors[node1[i]];
                neighbor[j] = node2[i];
                edgeWeight2[j] = edgeWeight1[i];
                nNeighbors[node1[i]]++;
                j = firstNeighborIndex[node2[i]] + nNeighbors[node2[i]];
                neighbor[j] = node1[i];
                edgeWeight2[j] = edgeWeight1[i];
                nNeighbors[node2[i]]++;
            }

        if (modularityFunction == 1)
        {
            nodeWeight = new double[nNodes];
            for (i = 0; i < nEdges; i++)
                nodeWeight[neighbor[i]] += edgeWeight2[i];
            network = new Network(nNodes, firstNeighborIndex, neighbor, edgeWeight2, nodeWeight);
        }
        else
            network = new Network(nNodes, firstNeighborIndex, neighbor, edgeWeight2);

        return network;
    }

    private static void writeOutputFile(String fileName, int[] cluster) throws IOException
    {
        BufferedWriter bufferedWriter;
        int i;

        bufferedWriter = new BufferedWriter(new FileWriter(fileName));

        for (i = 0; i < cluster.length; i++)
        {
            bufferedWriter.write(Integer.toString(cluster[i]));
            bufferedWriter.newLine();
        }

        bufferedWriter.close();
    }
}
