package sf.net.nutchcontentexpoerter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;

import java.io.File;
import java.io.FileOutputStream;

/**
 *
 */
public class NutchContentExporter {

    public static void main(String[] args) {

        if (args.length != 2) {
            System.out.println("usage: segmentdir (-local | -dfs <namenode:port>) outputdir");
            return;
        }

        try {
            Configuration conf = NutchConfiguration.create();
            FileSystem fs = FileSystem.get(conf);

            String segment = args[0];

            File outDir = new File(args[1]);
            if (!outDir.exists()) {
                if (outDir.mkdir()) {
                    System.out.println("Creating output dir " + outDir.getAbsolutePath());
                }
            }

            Path file = new Path(segment, Content.DIR_NAME + "/part-00000/data");
            // new 2.0 API
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file));


            Text key = new Text();
            Content content = new Content();

            while (reader.next(key, content)) {
                String filename = key.toString().replaceFirst("http://", "").replaceAll("/", "___").trim();

                File f = new File(outDir.getCanonicalPath() + "/" + filename);
                FileOutputStream fos = new FileOutputStream(f);
                fos.write(content.getContent());
                fos.close();
                System.out.println(f.getAbsolutePath());
            }
            reader.close();
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}