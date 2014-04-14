package pmr;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;

public class Reducer {
	/*
	 * reducer: Class for managing Reduce phase of MapReduce Job
	 */
	public ArrayList<String> partitionFiles = null;
	private int reduce = 0;
	private String reduceFile = null;
	private PrintWriter reduceWrite = null;

	public Reducer(String[] args) throws Exception {
		/*
		 * Initializes Reduce task with parameters passed by MapReduce framework
		 * as command line parameters.
		 */
		this.partitionFiles = new ArrayList<String>();
		Collections.addAll(this.partitionFiles, args[0].split(":"));
		String[] tokens = this.partitionFiles.get(0).split("-");
		this.reduce = Integer.parseInt(tokens[tokens.length - 1]);

		this.reduceFile = "reduce-" + this.reduce;
		this.reduceWrite = new PrintWriter(reduceFile);
	}

	public void emit(Object key, Object value) {
		// Emit the key value pair to reduce file
		this.reduceWrite.println(key + "," + value);
	}

	public void emit(Object value) {
		// Emit the value to reduce file
		this.reduceWrite.println(value);
	}

	public void finalize() {
		// Close the reduce file
		this.reduceWrite.close();
	}

}