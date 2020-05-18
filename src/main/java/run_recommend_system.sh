chmod +x *.sh

echo "Put input text file into HDFS input/*"
hdfs dfs -mkdir -p input
hdfs dfs -put input/* input

echo "Create outputs directory to put final results and temp results"
hdfs dfs -mkdir -p outputs


hadoop com.sun.tools.javac.Main *.java
jar cf movie.jar *.class

hadoop jar movie.jar Driver input outputs/data outputs/co outputs/normalize outputs/multiplicate outputs/results

echo "Movie Recommendation results are in HDFS outputs/results"
