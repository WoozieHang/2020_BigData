import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
public class Map3_2 extends Mapper<Object,Text,Text,Text>
{
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map3_2      输出key：className                    输出value：vector
    //reducer3_2  输出key：className                  输出value：test vector (20%)
    @Override
    public void map(Object key,Text value,Context context) throws IOException,InterruptedException
    {

        String str=value.toString();
        String vector=str.split("\t")[1];
        String className=str.split("\t")[0];
        context.write(new Text(className),new Text(vector));
    }
}

