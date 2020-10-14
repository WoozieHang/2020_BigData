import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Combiner2_1 extends Reducer<Text,Text,Text,Text>
{
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map2_1 输出key：word;className;textName;          输出value：1
    //Combiner2_1 输出key：className;textName          输出value：word;num
    //reducer2_1 输出key：className;textName           输出value：word;freq

    private static Text word=new Text();
    private static Text index=new Text();
    @Override
    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException,InterruptedException
    {
        //key完全相同，所有的作品相同
        int num=0;
        for (Text v:value){
            num++;
        }
        word.set(key.toString().split("&")[1]+"\t"+key.toString().split("&")[2]);
        index.set(key.toString().split("&")[0]+"&"+num);
        context.write(word,index);
    }
}

