import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class Reduce1_1 extends Reducer<Text,Text,Text,Text>{
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map1_1 输出key：词语;className;textName          输出value：1
    //Combiner1_1 输出key：词语;className           输出value：textName
    //reducer1_1 输出key：词语                    输出value：Text次数 for certain class...

    private static Text index=new Text();
    private static Text word=new Text();
    @Override
    public void reduce(Text key,Iterable<Text> value,Context context) throws IOException,InterruptedException
    {
        int num=0;
        for(Text v:value) {
            num++;
        }
        word.set(key.toString().split("&")[0]);
        index.set(String.valueOf(num));
        context.write(word,index);
    }
}
