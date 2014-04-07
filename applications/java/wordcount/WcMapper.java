package pmr;

import java.io.BufferedReader;
import java.io.FileReader;

import pmr.Mapper;

public class WcMapper {
	public static void main(String[] args) throws Exception {

		// Initialize Map Job
		Mapper mapJob = new Mapper(args);

		// Map function
		BufferedReader br = new BufferedReader(new FileReader(mapJob.chunkFile));
		String line;

		while ((line = br.readLine()) != null) {
			for (String word : line.split(" ")) {
				mapJob.emit(word, 1);
			}
		}
		br.close();

		// Finalize map job
		mapJob.finalize();

	}
}
