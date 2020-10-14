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

public class Map1_3 extends Mapper<Object,Text,DoubleWritable,Text>
{
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map1_3      输出key：information entropy                    输出value：词语 \t total_num
    //reducer1_3  输出key：information entropy                    输出value：词语 \t total_num

    @Override
    public void map(Object key,Text value,Context context) throws IOException,InterruptedException
    {

        String str=value.toString();
        String word=str.split("\t")[0];
        double entropy=Double.parseDouble(str.split("\t")[1]);
        String total_num=str.split("\t")[2];
        context.write(new DoubleWritable(entropy),new Text(word+"\t"+total_num));
    }
}
