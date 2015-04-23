package sample;

import java.io.IOException;

import core.ModularityOptimizer;

public class Sample_dSLM_Run {
	public static void main(String[] args) throws IOException {
		
		/*****************************************************************************************
		Prior to running this class, the sample input file must be pre-processed. 
		Therefore, prior to running this class, you need to run the Sample_dSLM_Preprocess class.
		******************************************************************************************/
		
		ModularityOptimizer modularityOptimizer = new ModularityOptimizer();
		
		//SLM run for 300,000 edges
		modularityOptimizer.detectCommunities("sample_input_data\\hep-th-citationsSLMSorted300000SLMSorted.txt", "sample_input_data\\hep-th-citationsSLMSorted300000SLMSorted.txt", 1, 1.0, 3, 1, 10, 0L, true, false, null, 1);
		
		//SLM runs for 301,000 and 310,000 edges
		modularityOptimizer.detectCommunities("sample_input_data\\hep-th-citationsSLMSorted301000SLMSorted.txt", "sample_input_data\\hep-th-citationsSLMSorted301000SLMSorted.txt", 1, 1.0, 3, 1, 10, 0L, true, false, null, 1);
		modularityOptimizer.detectCommunities("sample_input_data\\hep-th-citationsSLMSorted310000SLMSorted.txt", "sample_input_data\\hep-th-citationsSLMSorted310000SLMSorted.txt", 1, 1.0, 3, 1, 10, 0L, true, false, null, 1);
		
		//dSLM runs for 301,000 and 310,000 edges by using the SLM result of 300,000 edges as the starting point
		modularityOptimizer.detectCommunities("sample_input_data\\hep-th-citationsSLMSorted301000SLMSorted.txt", "sample_input_data\\hep-th-citationsSLMSorted301000SLMSorted.txt", 1, 1.0, 3, 1, 10, 0L, true, true, "sample_input_data\\hep-th-citationsSLMSorted300000SLMSorted-static.txt", 1);
		modularityOptimizer.detectCommunities("sample_input_data\\hep-th-citationsSLMSorted310000SLMSorted.txt", "sample_input_data\\hep-th-citationsSLMSorted310000SLMSorted.txt", 1, 1.0, 3, 1, 10, 0L, true, true, "sample_input_data\\hep-th-citationsSLMSorted300000SLMSorted-static.txt", 1);
		
		//SLM runs for 299,000 and 290,000 edges
		modularityOptimizer.detectCommunities("sample_input_data\\hep-th-citationsSLMSorted299000SLMSorted.txt", "sample_input_data\\hep-th-citationsSLMSorted299000SLMSorted.txt", 1, 1.0, 3, 1, 10, 0L, true, false, null, 1);
		modularityOptimizer.detectCommunities("sample_input_data\\hep-th-citationsSLMSorted290000SLMSorted.txt", "sample_input_data\\hep-th-citationsSLMSorted290000SLMSorted.txt", 1, 1.0, 3, 1, 10, 0L, true, false, null, 1);
		
		//dSLM runs for 299,000 and 290,000 edges by using the SLM result of 300,000 edges as the starting point
		modularityOptimizer.detectCommunities("sample_input_data\\hep-th-citationsSLMSorted299000SLMSorted.txt", "sample_input_data\\hep-th-citationsSLMSorted299000SLMSorted.txt", 1, 1.0, 3, 1, 10, 0L, true, true, "sample_input_data\\hep-th-citationsSLMSorted300000SLMSorted-static.txt", 1);
		modularityOptimizer.detectCommunities("sample_input_data\\hep-th-citationsSLMSorted290000SLMSorted.txt", "sample_input_data\\hep-th-citationsSLMSorted290000SLMSorted.txt", 1, 1.0, 3, 1, 10, 0L, true, true, "sample_input_data\\hep-th-citationsSLMSorted300000SLMSorted-static.txt", 1);
	}
}
