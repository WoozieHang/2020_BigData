import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce3_5 extends Reducer<Text,Text,Text,Text> {
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map3_5 输出key：actual className      输出value：forecast className
    //reducer3_5 输出key：actual className     输出value：forecast className

    @Override
    public void reduce(Text key,Iterable<Text> value,Context context) throws IOException,InterruptedException {
        for(Text v:value) {
            context.write(key,v);
        }
    }
}