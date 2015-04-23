package sample;

import java.io.IOException;

import preprocess.InputNetworkPreprocess;

public class Sample_dSLM_Preprocess {
	public static void main(String[] args) throws IOException {
		InputNetworkPreprocess inputNetworkPreprocess = new InputNetworkPreprocess();
		
		inputNetworkPreprocess.convertTotalIntsToSLMInts("sample_input_data\\hep-th-citations.txt");
		inputNetworkPreprocess.slmConverter("sample_input_data\\hep-th-citationsSLM.txt");
		
		inputNetworkPreprocess.constructPartOf("sample_input_data\\hep-th-citationsSLMSorted.txt", 0, 290000);
		inputNetworkPreprocess.constructPartOf("sample_input_data\\hep-th-citationsSLMSorted.txt", 0, 299000);
		inputNetworkPreprocess.constructPartOf("sample_input_data\\hep-th-citationsSLMSorted.txt", 0, 300000);
		inputNetworkPreprocess.constructPartOf("sample_input_data\\hep-th-citationsSLMSorted.txt", 0, 301000);
		inputNetworkPreprocess.constructPartOf("sample_input_data\\hep-th-citationsSLMSorted.txt", 0, 310000);
	}
}
