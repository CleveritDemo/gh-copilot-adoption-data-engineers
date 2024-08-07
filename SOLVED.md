# Solved exercises

## RDD Operations

### Creating RDD from a list

```python
# Create an RDD from a list
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

# Create a collect action to distribute the data
distData = spark.sparkContext.parallelize(data)

# Perform a collect action to see the data
distData.collect()
```

### Creating RDD from a list of tuples

```python
# Create an RDD from a list of tuples with name and age between 20 and 49
data = [('Alice', 34), ('Bob', 45), ('Charlie', 23), ('David', 49), ('Alice', 28)]
distData = spark.sparkContext.parallelize(data)

# Perform a collect action to see the data
distData.collect()
```
### RDD Map Transformation

```python
# Map transformation: Convert name to uppercase
mappedData = distData.map(lambda x: (x[0].upper(), x[1]))

# Perform a collect action to see the data
mappedData.collect()
```

### RDD Filter Transformation

```python
# Filter transformation: Filter records with age greater than 30
filteredData = mappedData.filter(lambda x: x[1] > 30)

# Perform a collect action to see the data
filteredData.collect()
```

### RDD ReduceByKey Transformation

```python
# ReduceByKey: Calculate the total age for each name
reducedData = filteredData.reduceByKey(lambda x, y: x + y)

# Perform a collect action to see the data
reducedData.collect()
```

### RDD SortyBy Transformation

```python
# SortyBy Transformation: Sort the data by age in descending order
sortedData = reducedData.sortBy(lambda x: x[1], ascending=False)

# Perform a collect action to see the data
sortedData.collect()
```