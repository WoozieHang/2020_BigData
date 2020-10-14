import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Vector;

public class Reduce3_1  extends Reducer<Text,Text,Text,Text>{
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map3_1      输出key：className                    输出value：vector
    //reducer3_1  输出key：className                  输出value：training vector (80%)

    @Override
    public void reduce(Text key,Iterable<Text> value,Context context) throws IOException,InterruptedException
    {
        int count=0;
        for(Text v:value) {
            count+=1;
            if(count%5!=0)
                context.write(key,v);
        }

    }
}
