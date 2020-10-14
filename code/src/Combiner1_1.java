import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Combiner1_1 extends Reducer<Text,Text,Text,Text>
{
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map1_1 输出key：词语;className;textName          输出value：1
    //Combiner1_1 输出key：词语;className           输出value：textName
    //reducer1_1 输出key：词语                    输出value：Text次数 for certain class...

    private static Text word=new Text();
    private static Text index=new Text();
    @Override
    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException,InterruptedException
    {
        //key完全相同，所有的作品相同
        word.set(key.toString().split("&")[0]+"&"+key.toString().split("&")[1]);
        index.set(key.toString().split("&")[2]);
        context.write(word,index);
    }
}
