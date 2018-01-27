import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

class MyReducer : Reducer<Text,IntWritable, Text, IntWritable>() {
    override fun reduce(key: Text?, values: MutableIterable<IntWritable>?, context: Context?) {
        val sum = IntWritable(values?.sumBy { it.get() }!!)
        context?.write(key, sum)
    }
}