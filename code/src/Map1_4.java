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

public class Map1_4  extends Mapper<Object,Text,Text,Text>
{
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map1_4      输出key：词语                    输出value：total_num
    //reducer1_4  输出key：词语                    输出value：total_num
    private static int lineCount=0;
    @Override
    public void map(Object key,Text value,Context context) throws IOException,InterruptedException
    {
        if(lineCount<50000) {
            String str = value.toString();
            String word = str.split("\t")[1];
            String total_num = str.split("\t")[2];
            context.write(new Text(word), new Text(total_num));
            lineCount++;
        }

        /*
        String str = value.toString();
        String word = str.split("\t")[1];
        String total_num = str.split("\t")[2];
        context.write(new Text(word), new Text(total_num));
        */
    }
}
