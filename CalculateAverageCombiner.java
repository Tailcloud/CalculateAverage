package calculateAverage;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalculateAverageCombiner extends Reducer<Text,SumCountPair,Text,SumCountPair> {

    public void reduce(Text key, Iterable<SumCountPair> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		int count = 0;
		
		for (SumCountPair val: values) {
	      sum += val.getSum();
				count++;
		}
		
		context.write(key, new SumCountPair(sum, count));
	}
}
