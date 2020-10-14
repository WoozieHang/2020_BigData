import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map3_4 extends Mapper<Object, Text,Text,Text>
{
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map3_4      输出key：className                    输出value：vector
    //reducer3_4  输出key：className                  输出value：totalClassNum;mean vector;variance vector
    @Override
    public void map(Object key,Text value,Context context) throws IOException,InterruptedException
    {

        String str=value.toString();
        String className=str.split("\t")[0];
        String vector=str.split("\t")[1];
        context.write(new Text(className),new Text(vector));
    }
}
