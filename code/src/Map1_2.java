import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.util.HashSet;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Map1_2 extends Mapper<Object,Text,Text,Text>
{
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map1_2 输出key：词语             输出value：次数 for certain class
    //reducer1_2 输出key：词语                    输出value：[information entropy][\t][total mail num]

    @Override
    public void map(Object key,Text value,Context context) throws IOException,InterruptedException
    {
        String str=value.toString();
        int a=str.indexOf('\t');
        String word=str.substring(0,a);
        String num=str.substring(a+1);
        context.write(new Text(word),new Text(num));
    }
}
