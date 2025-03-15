# How to Run the Code

1. **Compile the Code**  
    Use the `javac` command to compile the Java files. For example:
    ```bash
    javac MapReduceFiles.java
    ```

2. **Run the Program**  
    Use the `java` command to execute the program. Provide the required arguments, such as the input file:
    ```bash
    java MapReduceFiles text1.txt text2.txt text3.txt text4.txt text4.txt text5.txt text6.txt text7.txt text8.txt text9.txt text10.txt
    ```

3. **Expected Output**  
    The program will process the input file the text files and display the results in the console or as specified in the code.

4. **Additional Notes**  
    - Ensure that `text1` (or your input file) is in the same directory as the compiled `.class` files.
    - If there are dependencies or additional files, make sure they are properly set up.

5. **Expected Output**
    ```bash
    File Reading Time: 590 ms
    Brute Force Approach Time: 794 ms
    Map Phase Time (MapReduce): 1422 ms
    Group Phase Time (MapReduce): 190 ms
    Reduce Phase Time (MapReduce): 288 ms
    MapReduce Approach Time: 1903 ms
    Map Phase Time (Distributed): 651 ms
    Group Phase Time (Distributed): 234 ms
    Reduce Phase Time (Distributed): 11532 ms
    Distributed MapReduce Approach Time: 12420 ms
    ```