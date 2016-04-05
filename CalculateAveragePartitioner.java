package calculateAverage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CalculateAveragePartitioner extends Partitioner<Text, SumCountPair> {
	@Override
	public int getPartition(Text key, SumCountPair value, int numReduceTasks) {
		
		if(key.toString().substring(0,1).matches("^[A-Ga-g]")){
			return 0;
		}else if(key.toString().substring(0,1).matches("^[H-Zh-z]")){
			return 1;
		}
		return 0;
	}
}