import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text


fun main(args: Array<String>) {
    val conf = Configuration()
    val job = Job(conf, "WordCount")
    job.mapperClass = MyMapper::class.java
    job.reducerClass = MyReducer::class.java
    job.inputFormatClass = TextInputFormat::class.java
    job.outputFormatClass = TextOutputFormat::class.java
    job.outputKeyClass = Text::class.java
    job.outputValueClass = IntWritable::class.java

    val outputPath = Path(args[1])
//Configuring the input/output path from the filesystem into the job
    FileInputFormat.addInputPath(job, Path(args[0]))
    FileOutputFormat.setOutputPath(job, Path(args[1]))
//deleting the output path automatically from hdfs so that we don't have to delete it explicitly
    outputPath.getFileSystem(conf).delete(outputPath)
//exiting the job only if the flag value becomes false
    System.exit(if (job.waitForCompletion(true)) 0 else 1)



}
