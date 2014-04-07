package pmr;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;

public class Mapper {
	/*
	 * mapper: Class for managing Map phase of MapReduce Job
	 */

	public String chunkFile = null;
	private int nbrReduces = 1;
	private PrintWriter[] partitionFile = null;
	private ArrayList<ArrayList<String>> partitionList = null;
	public String[] mapArgs = null;

	public Mapper(String[] args) {
		/*
		 * Initializes Map task with parameters passed by MapReduce framework as
		 * command line parameters.
		 */
		
		// the 1st argument is a comma separated list of chunk files.
		this.chunkFile = args[0];
		this.nbrReduces = Integer.parseInt(args[1]);
		this.partitionFile = new PrintWriter[nbrReduces];
		this.partitionList = new ArrayList<ArrayList<String>>();

		// user defined map task arguments
		if (args.length > 2)
			System.arraycopy(args, 3, this.mapArgs, 0, args.length - 3);
	}

	private int partition(String key) {
		/*
		 * Default partition function. This function could be overwritten by
		 * custom partition functions.
		 * 
		 * @param key: map task emitted key value
		 * 
		 * @return: partition number into which the key,value pair has to be
		 * written to
		 */

		return (int) (key.hashCode() % nbrReduces);
	}

	public void emit(Object key, Object value) {
		// Emit the key value based on the partition function
		this.partitionList.get(this.partition(key.toString())).add(
				key + "," + value);
	}

	public void finalize() throws Exception {
		/*
		 * Prepare the map output files sort the map output contents
		 */

		// open partition file for each reduce
		for (int i = 0; i < this.nbrReduces; i++) {
			String partitionName = "partition-" + i;
			this.partitionFile[i] = new PrintWriter(this.chunkFile
					+ "-sorted-map-" + partitionName);
		}

		// sort each partition list and write to file
		for (int i = 0; i < this.nbrReduces; i++) {
			Collections.sort(this.partitionList.get(i));
			for (String line : this.partitionList.get(i)) {
				this.partitionFile[i].println(line);
			}
		}

		// close all partition files
		for (int i = 0; i < this.nbrReduces; i++) {
			this.partitionFile[i].close();
		}
	}
}
