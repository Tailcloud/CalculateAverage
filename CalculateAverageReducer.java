package calculateAverage;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import sun.awt.SunHints.Value;

public class CalculateAverageReducer extends Reducer<Text, SumCountPair, Text, DoubleWritable> {
	
    public void reduce(Text key, Iterable<SumCountPair> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		int count = 0;

		for (SumCountPair val: values) {
			sum += val.getSum();
			count += val.getCount();
		}
		context.write(key, new DoubleWritable((double)sum/count));
		
	}
}
