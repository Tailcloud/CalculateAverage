package calculateAverage;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CalculateAverageMapper extends Mapper<LongWritable, Text, Text, SumCountPair> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString(), "\t");

		while (itr.hasMoreTokens()) {

			  String toProcess = itr.nextToken();
			  int sum = Integer.parseInt(itr.nextToken());
			  if(toProcess.matches("^[a-zA-Z]*")){
				  context.write(new Text(toProcess), new SumCountPair(sum, 1));

			  }else{

			  }
		}
	}
}
