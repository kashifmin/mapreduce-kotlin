import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.Reporter
import java.util.*

class MyMapper : Mapper<LongWritable, Text, Text, IntWritable>() {

    override fun map(key: LongWritable?, value: Text?, context: Context?) {
        val tokenizer = StringTokenizer(value.toString())
        while (tokenizer.hasMoreTokens()) {
            value?.set(tokenizer.nextToken())
            context?.write(value, IntWritable(1))
        }
    }
}