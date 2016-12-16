import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
public class SequenceFileWrite {

	/**
	 * @param args
	 * @author Nagamallikarjuna
	 */
	public static void main(String[] args) throws IOException, URISyntaxException {
		String name = "/home/hadoop/bigdata/SparkApps/RDD_API_Calls/stocks";
		@SuppressWarnings("resource")
		BufferedReader br = new BufferedReader(new FileReader(name));
		String line = br.readLine();
		String localpath = "file:///home/hadoop/bigdata/SparkApps/RDD_API_Calls/stocks.seq";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.getLocal(conf);
		Path path = new Path(localpath);
		Text key = new Text();
		LongWritable value = new LongWritable();
		SequenceFile.Writer writer = null;
		try 
		{
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
			while(line != null)
			{
				String parts[] = line.split("\\t");
				key.set(parts[1]);
				value.set(Long.valueOf(parts[7]));
				writer.append(key, value);
				line = br.readLine();
			}
			
		}
		finally {
			IOUtils.closeStream(writer);
		}
	}
}
