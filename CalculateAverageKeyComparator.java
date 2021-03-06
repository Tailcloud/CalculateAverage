package calculateAverage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CalculateAverageKeyComparator extends WritableComparator {
	
	public CalculateAverageKeyComparator() {
		super(Text.class, true);
	}
	
	
	public int compare(WritableComparable o1, WritableComparable o2) {
		Text key1 = (Text) o1;
		Text key2 = (Text) o2;
		
		String text1 = key1.toString();
		String text2 = key2.toString();
		
		return text1.compareTo(text2);
		
	}
}