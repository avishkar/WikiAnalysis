
package org.myorg;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.bzip2.CBZip2InputStream;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;



/**
 * Reads records that are delimited by a specifc begin/end tag.
 */


public class Hw1 {

	public static class MapA extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value1, OutputCollector<Text,Text>output, Reporter reporter) throws IOException {
			String xmlString;
			SAXBuilder builder;
			Reader in;
			Document doc;
			Element root;
			String title,text,target_page_id,source_page_id;
			Pattern pattern,pattern2;
			Matcher m,m1;

			xmlString = value1.toString();
			builder = new SAXBuilder();
			in = new StringReader(xmlString);
			try {

				doc = builder.build(in);
				root = doc.getRootElement();
				title =root.getChild("title").getText() ;
				text =root.getChild("revision").getChild("text").getText();
				source_page_id = title+",";
				pattern = Pattern.compile("\\[\\[([^:]+?)\\]\\]");
				m = pattern.matcher(text);
				while(m.find()) {
					pattern2 = Pattern.compile("(.+?)\\|(.+?)");
					m1= pattern2.matcher(m.group(1));
					if(m1.find())
						target_page_id=m1.group(1);
					else
						target_page_id=m.group(1);

					output.collect(new Text(source_page_id), new Text(target_page_id));
				}
			} catch (JDOMException ex) {
				Logger.getLogger(MapA.class.getName()).log(Level.SEVERE, null, ex);
			} catch (IOException ex) {
				Logger.getLogger(MapA.class.getName()).log(Level.SEVERE, null, ex);
			}

		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf;
		JobConf job;
		String input = "",output = "";
		
		if(args.length!=2)
			System.out.println("Invalid number of input");
		else{
			input=args[0]; //"./input/*"
			output=args[1];
		
		}


		String input2="./tempin/";
		File folder = new File(input);
		File[] listOfFiles = folder.listFiles();
	
		for (File listOfFile : listOfFiles){
			if (!listOfFile.isDirectory()){
		
				FileInputStream fileInputStream = new FileInputStream(input+listOfFile.getName());
				fileInputStream.read();
				fileInputStream.read();
				CBZip2InputStream cin = new CBZip2InputStream(fileInputStream);
				
				FileOutputStream decOut = new FileOutputStream(input2+listOfFile.getName());



				byte[] buf = new byte[300000];
				int len;

				while((len = cin.read(buf))>0){
					decOut.write(buf, 0, len);
				}
				decOut.close();
				cin.close();
			}
		}

		System.out.println("4");
		conf = new Configuration();
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
		job = new JobConf(conf, Hw1.class);
		job.setJarByClass(MapA.class);
		job.setInputFormat(XmlInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setNumReduceTasks(0);
		FileInputFormat.setInputPaths(job, new Path(input2));
		//if it doesnt try the following: FileInputFormat.addInputPath(job, new Path("./input/*"));
		//it works http://stackoverflow.com/questions/10210713/hadoop-mapreduce-provide-nested-directories-as-job-input
		//http://stackoverflow.com/questions/4792926/hadoop-input-files-order
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputKeyClass(Text.class); //Key text
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(MapA.class);
		JobClient.runJob(job);
	}
}