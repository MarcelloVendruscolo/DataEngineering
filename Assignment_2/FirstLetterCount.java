//package org.myorg;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class FirstLetterCount {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
	    private String a_word;
	    
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
		    this.a_word = tokenizer.nextToken();
                word.set(first_letter(lowerCase(this.a_word)));
                output.collect(word, one);
            }
        }

	public String lowerCase (String word) {
		if (word.length() != 0 | word != null) {
			char[] letters = new char[word.length()];
			for (int index = 0; index < word.length(); index++) {
				letters[index] = word.charAt(index);
			}
                	if (Character.isLetter(letters[0])) {
                        	letters[0] = Character.toLowerCase(letters[0]);
                	}
                	String minuscleWord = new String(letters);
                	return minuscleWord;
		}
		return word;
	}

	public String first_letter (String word) {
		if (word.length() != 0 | word != null) {
			char[] letters = new char[word.length()];
                        for (int index = 0; index < word.length(); index++) {
                                letters[index] = word.charAt(index);
                        }
			String first_letter = Character.toString(letters[0]);
			return first_letter;
		}
		return word;
	}

    }
	
    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }
	
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(FirstLetterCount.class);
        conf.setJobName("firstlettercount");
	    
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
	    
        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
	    
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
	    
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	    
        JobClient.runJob(conf);
    }
}
