import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Vector;

public class Map3_3 extends Mapper<Object, Text,Text,Text>
{
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map3_3 输出key：actual className      输出value：forecast className
    //reducer3_3 输出key：actual className      输出value：forecast className

    private static Vector<String>vectorClassName=new Vector<String>();
    private static Vector<String>vectorIndex=new Vector<String>();
    private static Vector<String>vectorContent=new Vector<String>();
    private Path[] localFiles;

    @Override
    public void setup(Context context) throws IOException,InterruptedException{
        Configuration conf=context.getConfiguration();
        localFiles= context.getLocalCacheFiles();
        //将缓存文件内容读入到当前Map Task的全局变量中
        String lineStr;
        BufferedReader br=new BufferedReader(new FileReader(localFiles[0].toString()));
        while((lineStr=br.readLine())!=null){
            vectorClassName.add(lineStr.split("\t")[0]);
            vectorIndex.add(lineStr.split("\t")[1].split("&")[1]);
            vectorContent.add(lineStr.split("\t")[1].split("&")[0]);
        }
        br.close();
    }

    @Override
    public void map(Object key,Text value,Context context) throws IOException,InterruptedException
    {
        String str=value.toString();
        String testClassName=str.split("\t")[0];
        String testVectorContent=str.split("\t")[1].split("&")[0];
        String testVectorIndex=str.split("\t")[1].split("&")[1];

        int trainingSetNum=vectorClassName.size();
        int k=(int)Math.sqrt(trainingSetNum);
        Vector<String> vote=new Vector<String>();
        for(int i=0;i<trainingSetNum;i++){
            String []aContent=vectorContent.get(i).split(",");
            String []bContent=testVectorContent.split(",");
            String []aIndex=vectorIndex.get(i).split(",");
            String []bIndex=testVectorIndex.split(",");

            double dst=0;
            int aj=0,bj=0;
            while(aj<aIndex.length && bj<bIndex.length){
                int aindex=Integer.parseInt(aIndex[aj]);
                int bindex=Integer.parseInt(bIndex[bj]);
                if(aindex<bindex){
                    double acontent=Double.parseDouble(aContent[aj]);
                    dst+=acontent*acontent;
                    aj+=1;
                }
                else if(aindex>bindex){
                    double bcontent=Double.parseDouble(bContent[bj]);
                    dst+=bcontent*bcontent;
                    bj+=1;
                }
                else{
                    double abcontent=Double.parseDouble(aContent[aj])-Double.parseDouble(bContent[bj]);;
                    dst+=abcontent*abcontent;
                    aj+=1;
                    bj+=1;
                }
            }
            while(aj<aIndex.length){
                double acontent=Double.parseDouble(aContent[aj]);
                dst+=acontent*acontent;
                aj++;
            }
            while(bj<bIndex.length){
                double bcontent=Double.parseDouble(bContent[bj]);
                dst+=bcontent*bcontent;
                bj++;
            }

            double param1=1;
            double param2=0.18;
            vote.add(String.valueOf(param1*Math.exp((-1*dst)/param2)));
        }
        Vector<Integer> max_k_id=new Vector<Integer>();
        for(int i=0;i<k;i++){
            max_k_id.add(i);
        }
        for(int i=k;i<trainingSetNum;i++){
            double tmp=Double.parseDouble(vote.get(i));
            int current_min_id=0;
            double current_min_v=Double.parseDouble(vote.get(max_k_id.get(0)));
            for(int j=1;j<k;j++){
                double tmp_v=Double.parseDouble(vote.get(max_k_id.get(j)));
                if(tmp_v<current_min_v){
                    current_min_v=tmp_v;
                    current_min_id=j;
                }
            }
            if(tmp>current_min_v){
                max_k_id.set(current_min_id,i);
            }
        }
        Vector<String> reducedClassName=new Vector<String>();
        Vector<Double>reducedVote=new Vector<Double>();
        for(int i=0;i<k;i++){
            int id=max_k_id.get(i);
            String classStr=vectorClassName.get(id);
            Double d=Double.parseDouble(vote.get(id));
            int matched=-1;
            for(int j=0;j<reducedClassName.size();j++){
                if(reducedClassName.get(j).equals(classStr)) {
                    matched = j;
                    break;
                }
            }
            if(matched==-1){
                reducedClassName.add(classStr);
                reducedVote.add(d);
            }
            else{
                reducedVote.set(matched,reducedVote.get(matched)+d);
            }
        }
        int max_id=0;
        double max_v=reducedVote.get(0);
        for(int i=1;i<reducedVote.size();i++){
            double tmp_v=reducedVote.get(i);
            if(tmp_v>max_v) {
                max_v = tmp_v;
                max_id=i;
            }
        }

        context.write(new Text(testClassName),new Text(reducedClassName.get(max_id)));

    }
}

