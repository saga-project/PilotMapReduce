package pmr;
import java.io.*;
import java.util.*;
import pmr.Reducer;

public class WcReducer {
	
	public static String join(String[] words, String delimiter )
	{
    	StringBuilder sb = new StringBuilder();
    	int i=0;
    	for (;i<words.length-1;i++)
    	{
    		sb.append(words[i] + ",");		        		
    	}
    	
    	sb.append(words[i]);
    	
    	return sb.toString();
	}

	public static void main(String[] args) throws Exception{

		    // Initialize Reduce job
		    Reducer reduceJob = new Reducer(args);         
		    
		    // Reduce function    
		    Map<String, Integer> count = new HashMap<String, Integer>();
		    
		    // split the map emitted to get words count from each partition file
		    for (String pName: reduceJob.partitionFiles)
		    {		    
		        BufferedReader inFile = new BufferedReader(new FileReader(pName));
		        String line;
		        String[] tokens;
		        while( (line = inFile.readLine()) != null )
		        {
		        	// Each line is delimited by comma by MR framework.
		        	tokens = line.split(",");
		        	
		        	// Actual word might contain "," and count is last token.
		        	int value = Integer.parseInt(tokens[tokens.length-1]);
		        	String word = join(Arrays.copyOfRange(tokens, 0, tokens.length-1), ",");
		        	
		        	if (count.containsKey(word))
		        		count.put(word, count.get(word) + value);
		        	else
		        		count.put(word, value);
		        }
		        inFile.close();
		    }
		    for(Map.Entry<String, Integer> e: count.entrySet())
		    	reduceJob.emit(e.getKey(),e.getValue());

		    // Finalize reduce job   
		    reduceJob.finalize(); 
		    
	}
}
