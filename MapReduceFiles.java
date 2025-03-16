import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.Scanner;

public class MapReduceFiles {

  public static void main(String[] args) {

    if (args.length < 1) { // Accept at least one file
      System.err.println("usage: java MapReduceFiles file1.txt [file2.txt ... fileN.txt]");
      System.exit(1);
    }

    long fileReadStartTime = System.currentTimeMillis(); // Start timing file reading
    Map<String, String> input = new HashMap<String, String>();
    try {
      for (String file : args) { // Dynamically handle all input files
        input.put(file, readFile(file));
      }
    } catch (IOException ex) {
      System.err.println("Error reading files...\n" + ex.getMessage());
      ex.printStackTrace();
      System.exit(0);
    }
    long fileReadEndTime = System.currentTimeMillis(); // End timing file reading
    System.out.println("File Reading Time: " + (fileReadEndTime - fileReadStartTime) + " ms");

    // APPROACH #1: Brute force
    {
      long startTime = System.currentTimeMillis(); // Start timing

      Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

      Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
      while(inputIter.hasNext()) {
        Map.Entry<String, String> entry = inputIter.next();
        String file = entry.getKey();
        String contents = entry.getValue();

        String[] words = contents.trim().split("\\s+");

        for(String word : words) {

          Map<String, Integer> files = output.get(word);
          if (files == null) {
            files = new HashMap<String, Integer>();
            output.put(word, files);
          }

          Integer occurrences = files.remove(file);
          if (occurrences == null) {
            files.put(file, 1);
          } else {
            files.put(file, occurrences.intValue() + 1);
          }
        }
      }

      long endTime = System.currentTimeMillis(); // End timing
      System.out.println("Brute Force Approach Time: " + (endTime - startTime) + " ms");
      //System.out.println("Brute Force Output: " + output);
    }


    // APPROACH #2: MapReduce
    {
      long startTime = System.currentTimeMillis(); // Start timing

      Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

      // MAP:
      long mapStartTime = System.currentTimeMillis();
      List<MappedItem> mappedItems = new LinkedList<MappedItem>();

      Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
      while(inputIter.hasNext()) {
        Map.Entry<String, String> entry = inputIter.next();
        String file = entry.getKey();
        String contents = entry.getValue();

        map(file, contents, mappedItems);
      }
      long mapEndTime = System.currentTimeMillis();
      System.out.println("Map Phase Time (MapReduce): " + (mapEndTime - mapStartTime) + " ms");

      // GROUP:
      long groupStartTime = System.currentTimeMillis();
      Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

      Iterator<MappedItem> mappedIter = mappedItems.iterator();
      while(mappedIter.hasNext()) {
        MappedItem item = mappedIter.next();
        String word = item.getWord();
        String file = item.getFile();
        List<String> list = groupedItems.get(word);
        if (list == null) {
          list = new LinkedList<String>();
          groupedItems.put(word, list);
        }
        list.add(file);
      }
      long groupEndTime = System.currentTimeMillis();
      System.out.println("Group Phase Time (MapReduce): " + (groupEndTime - groupStartTime) + " ms");

      // REDUCE:
      long reduceStartTime = System.currentTimeMillis();
      Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
      while(groupedIter.hasNext()) {
        Map.Entry<String, List<String>> entry = groupedIter.next();
        String word = entry.getKey();
        List<String> list = entry.getValue();

        reduce(word, list, output);
      }
      long reduceEndTime = System.currentTimeMillis();
      System.out.println("Reduce Phase Time (MapReduce): " + (reduceEndTime - reduceStartTime) + " ms");

      long endTime = System.currentTimeMillis(); // End timing
      System.out.println("MapReduce Approach Time: " + (endTime - startTime) + " ms");
      //System.out.println("MapReduce Output: " + output);
    }


    // APPROACH #3: Distributed MapReduce
    {
      long startTime = System.currentTimeMillis(); // Start timing

      final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

      // MAP:
      long mapStartTime = System.currentTimeMillis();
      final List<MappedItem> mappedItems = new LinkedList<MappedItem>();

      final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
        @Override
        public synchronized void mapDone(String file, List<MappedItem> results) {
          mappedItems.addAll(results);
        }
      };

      // Split files into chunks of lines for threads
      List<String[]> chunks = new ArrayList<>();
      int linesPerThread = 2000; // Example value, can be adjusted between 1000 and 10000
      for (Map.Entry<String, String> entry : input.entrySet()) {
        String file = entry.getKey();
        String contents = entry.getValue();
        String[] lines = contents.split("\\r?\\n"); // Split file into lines

        List<String> currentChunk = new ArrayList<>();
        for (String line : lines) {
          // Split lines longer than 80 characters
          while (line.length() > 60) {
            int splitIndex = line.lastIndexOf(' ', 60);
            if (splitIndex == -1) splitIndex = 60; // No whitespace, split at 80
            currentChunk.add(line.substring(0, splitIndex));
            line = line.substring(splitIndex).trim();
          }
          currentChunk.add(line);

          // Add chunk to list if it reaches the desired size
          if (currentChunk.size() >= linesPerThread) {
            chunks.add(currentChunk.toArray(new String[0]));
            currentChunk.clear();
          }
        }
        // Add remaining lines as a chunk
        if (!currentChunk.isEmpty()) {
          chunks.add(currentChunk.toArray(new String[0]));
        }
      }

      // Create threads for each chunk
      List<Thread> mapCluster = new ArrayList<>(chunks.size());
      for (String[] chunk : chunks) {
        Thread t = new Thread(new Runnable() {
          @Override
          public void run() {
            StringBuilder chunkContents = new StringBuilder();
            for (String line : chunk) {
              chunkContents.append(line).append("\n");
            }
            map("chunk", chunkContents.toString(), mapCallback);
          }
        });
        mapCluster.add(t);
        t.start();
      }

      // Wait for mapping phase to be over
      for (Thread t : mapCluster) {
        try {
          t.join();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      long mapEndTime = System.currentTimeMillis();
      System.out.println("Map Phase Time (Distributed): " + (mapEndTime - mapStartTime) + " ms");

      // GROUP:
      long groupStartTime = System.currentTimeMillis();
      Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

      Iterator<MappedItem> mappedIter = mappedItems.iterator();
      while(mappedIter.hasNext()) {
        MappedItem item = mappedIter.next();
        String word = item.getWord();
        String file = item.getFile();
        List<String> list = groupedItems.get(word);
        if (list == null) {
          list = new LinkedList<String>();
          groupedItems.put(word, list);
        }
        list.add(file);
      }
      long groupEndTime = System.currentTimeMillis();
      System.out.println("Group Phase Time (Distributed): " + (groupEndTime - groupStartTime) + " ms");

      // REDUCE:
      long reduceStartTime = System.currentTimeMillis();
      final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
        @Override
        public synchronized void reduceDone(String k, Map<String, Integer> v) {
          output.put(k, v);
        }
      };

      // Group words into chunks for threads
      List<Map.Entry<String, List<String>>> groupedEntries = new ArrayList<>(groupedItems.entrySet());
      List<List<Map.Entry<String, List<String>>>> reduceChunks = new ArrayList<>();
      int chunkSize = 0;
      List<Map.Entry<String, List<String>>> currentChunk = new ArrayList<>();

      for (Map.Entry<String, List<String>> entry : groupedEntries) {
        currentChunk.add(entry);
        chunkSize++;
        if (chunkSize >= 1000) { // Maximum chunk size
          reduceChunks.add(currentChunk);
          currentChunk = new ArrayList<>();
          chunkSize = 0;
        }
      }
      if (!currentChunk.isEmpty()) {
        reduceChunks.add(currentChunk);
      }

      // Create threads for each chunk
      List<Thread> reduceCluster = new ArrayList<>(reduceChunks.size());
      for (List<Map.Entry<String, List<String>>> chunk : reduceChunks) {
        Thread t = new Thread(new Runnable() {
          @Override
          public void run() {
            for (Map.Entry<String, List<String>> entry : chunk) {
              String word = entry.getKey();
              List<String> list = entry.getValue();
              reduce(word, list, reduceCallback);
            }
          }
        });
        reduceCluster.add(t);
        t.start();
      }

      // Wait for reducing phase to be over
      for (Thread t : reduceCluster) {
        try {
          t.join();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      long reduceEndTime = System.currentTimeMillis();
      System.out.println("Reduce Phase Time (Distributed): " + (reduceEndTime - reduceStartTime) + " ms");

      long endTime = System.currentTimeMillis(); // End timing
      System.out.println("Distributed MapReduce Approach Time: " + (endTime - startTime) + " ms");
      //System.out.println("Distributed MapReduce Output: " + output);
    }
  }

  public static void map(String file, String contents, List<MappedItem> mappedItems) {
    String[] words = contents.trim().split("\\s+");
    for (String word : words) {
        // Normalize the word: remove punctuation, symbols, and numbers
        word = word.replaceAll("[^a-zA-Z]", "").toLowerCase();
        if (!word.isEmpty()) { // Only include non-empty words
            mappedItems.add(new MappedItem(word, file));
        }
    }
  }

  public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
    Map<String, Integer> reducedList = new HashMap<String, Integer>();
    for(String file: list) {
      Integer occurrences = reducedList.get(file);
      if (occurrences == null) {
        reducedList.put(file, 1);
      } else {
        reducedList.put(file, occurrences.intValue() + 1);
      }
    }
    output.put(word, reducedList);
  }

  public static interface MapCallback<E, V> {

    public void mapDone(E key, List<V> values);
  }

  public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
    String[] words = contents.trim().split("\\s+");
    List<MappedItem> results = new ArrayList<MappedItem>(words.length);
    for(String word: words) {
      results.add(new MappedItem(word, file));
    }
    callback.mapDone(file, results);
  }

  public static interface ReduceCallback<E, K, V> {

    public void reduceDone(E e, Map<K,V> results);
  }

  public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

    Map<String, Integer> reducedList = new HashMap<String, Integer>();
    for(String file: list) {
      Integer occurrences = reducedList.get(file);
      if (occurrences == null) {
        reducedList.put(file, 1);
      } else {
        reducedList.put(file, occurrences.intValue() + 1);
      }
    }
    callback.reduceDone(word, reducedList);
  }

  private static class MappedItem {

    private final String word;
    private final String file;

    public MappedItem(String word, String file) {
      this.word = word;
      this.file = file;
    }

    public String getWord() {
      return word;
    }

    public String getFile() {
      return file;
    }

    @Override
    public String toString() {
      return "[\"" + word + "\",\"" + file + "\"]";
    }
  }

  private static String readFile(String pathname) throws IOException {
    File file = new File(pathname);
    StringBuilder fileContents = new StringBuilder((int) file.length());
    Scanner scanner = new Scanner(new BufferedReader(new FileReader(file)));
    String lineSeparator = System.getProperty("line.separator");

    try {
      if (scanner.hasNextLine()) {
        fileContents.append(scanner.nextLine());
      }
      while (scanner.hasNextLine()) {
        fileContents.append(lineSeparator + scanner.nextLine());
      }
      return fileContents.toString();
    } finally {
      scanner.close();
    }
  }

}