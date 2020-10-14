import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class Reduce1_4 extends Reducer<Text,Text,Text,Text>{
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map1_4      输出key：词语                    输出value：total_num
    //reducer1_4  输出key：词语                    输出value：total_num

    @Override
    public void reduce(Text key,Iterable<Text> value,Context context) throws IOException,InterruptedException {
        for(Text v:value) {
            context.write(key,v);
        }
    }
}
