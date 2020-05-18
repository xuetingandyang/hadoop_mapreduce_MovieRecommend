## Netflix Movies Recommender Systm with MapReduce

### Background
Item Collaborative Filtering (Item CF) is exploited for recommender system.

Item CF is a form of collaborative filtering based on the similarity between items 
calculated using people's ratings of those items.

**Reasons chosen Item CF:**
- For movies recommendation, the number of users is far larger than the number of items (movies).
- Item (Movie) will not change frequently, which can lower the number of calculations.
- In Item CF algorithm, we will exploit the user's historical data to recommend movies for this specific user. 
It is more convincing than the logic behind User CF algorithm.

**Steps in Item CF:**
- Data Preprocessing.
    * Mapper: Divide data by user_id
    * Reducer: Merge data for same user_id
- Build co-occurrence matrix
    * Mapper: For each user, find the number of occurrence of 'MovieA: MovieB' (always equal to 1)
    * Reducer: Merge and count the number of occurrence of 'MovieA: MovieB'
- Normalize co-occurrence matrix
    * Mapper: intput (movieA:movieB -> relation), output (movieA -> movieB:relation)
    * Reducer:  For each movieA, merge 'movieB:relation'. Output (movieB -> movieA=normalized_relation) 
- Build rating matrix   
    * Based on raw input, output (user_id -> movie_id:rate) 
- Matrix computation to get recommending results
    * MapReduce-1: finish the multiplication job
        * Mapper1-CooccurrenceMapper: read 'movieB -> movieA=normalized_relation'
        * Mapper2-RatingMapper: read 'user_id -> movie:rate'
        * Reducer: 
            input (movieB -> movieA:relation, user_id:rate)
            output (user_id:movie -> rate * relation)  
    * MapReduce-2: finish the sum job
        * Mapper: read (user_id:movie -> rate * relation, rate2 * relation2 ,...)  
        * Reducer: Merge and sum. Output (user_id:movie -> scores)

### Run Movie Recommendation System

Same as project "hadoop_mapreduce_auto_complete", we run this on docker container. The docker configuration is totally same.

In the container, start the Hadoop environment.
```shell script
./start-hadoop.sh
```

Run MovieRecommend project.
```shell script
cd src/main/java
./run_recommend_system.sh
```
