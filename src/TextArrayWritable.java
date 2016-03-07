//Copyright (c) 2015 University of Tampere

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

public class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
        
        @Override
        public String toString() {
          return Arrays.toString(get());
        }
        
}
