import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class Reduce1_3 extends Reducer<DoubleWritable,Text,DoubleWritable,Text>{
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map1_3      输出key：information entropy                    输出value：词语 \t total_num
    //reducer1_3  输出key：information entropy                    输出value：词语 \t total_num

    @Override
    public void reduce(DoubleWritable key,Iterable<Text> value,Context context) throws IOException,InterruptedException
    {
        for(Text v:value) {
            context.write(key,v);
        }

    }
}