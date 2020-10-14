import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map4 extends Mapper<Object, Text,Text,Text>
{
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map4_1      输出key：className                    输出value：1/0
    //reducer4_1  输出key：className                  输出value：correct rate
    @Override
    public void map(Object key,Text value,Context context) throws IOException,InterruptedException
    {

        String str=value.toString();
        String className=str.split("\t")[0];
        String forecastClassName=str.split("\t")[1];
        if(className.equals(forecastClassName))
            context.write(new Text(className),new Text("1"));
        else
            context.write(new Text(className),new Text("0"));
    }
}