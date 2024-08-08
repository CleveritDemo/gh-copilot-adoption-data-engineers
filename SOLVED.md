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

# DataFrame Operations

## Read CSV File into DataFrame

### Read CSV with header
```python
# Read CSV file into DataFrame
# Use ../data/products.csv file with header
csv_file = '../data/products.csv'
df = spark.read.csv(csv_file, header=True, inferSchema=True)

# Show the DataFrame schema
df.printSchema()

# Show the first 20 rows
df.show()
```

### Read CSV with an explicit schema definition
```python
# Import the necessary types
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# Define a new schema
schema = StructType([
    StructField('product_id', IntegerType(), False),
    StructField('product_name', StringType(), False),
    StructField('product_category_id', IntegerType(), False),
    StructField('product_description', StringType(), True),
    StructField('product_price', FloatType(), False)
])

# Load the data with the new schema
df = spark.read.csv(csv_file, header=True, schema=schema)

# Show the DataFrame schema
df.printSchema()

# Show the first 20 rows
df.show()
```

## Read JSON File into DataFrame

### Single line JSON
```python
# Read single line JSON
# Each row is a JSON record, records are separated by new line
json_file = '../data/products_singleline.json'
df = spark.read.json(json_file)

# Show the DataFrame schema
df.printSchema()

# Show the first 20 rows
df.show()
```

### Multi-lines JSON
```python
# Read multi-line JSON
# Use spark json method to read multi-line JSON with multiline option
json_file = '../data/products_multiline.json'
df = spark.read.json(json_file, multiLine=True)

# Show the DataFrame schema
df.printSchema()

# Show the first 20 rows
df.show()
```

## DataFrame Operations

### Loading the syntethic data into DataFrame
```python
# Load the synthetic data into a DataFrame
# Read with spark csv method
synthetic_file = '../data/stocks.txt'
df = spark.read.csv(synthetic_file, header=True, inferSchema=True)

# Show the DataFrame schema
df.printSchema()

# Show the first 20 rows
df.show()
```

### Select: Choose specific columns
```python
# Select specific columns from the DataFrame: name, category, and price
df.select('name', 'category', 'price').show()
```

### Filter: Apply conditions to filter rows
```python
# Filter rows based on a condition using filter method
df.filter(df['price'] > 100).show()
```

### GroupBy: Group data based on specific columns
```python
# Group by category and count the number of products in each category
df.groupBy('category').count().show()

# Add aggregation like sum, avg, max, min, etc.
df.groupBy('category').agg({'price': 'avg'}).show()
```

### Join: Combine multiple DataFrames based on specified columns
```python
# Join with another DataFrame. Create this new DF by filtering the original DF
df2 = df.filter(df['price'] > 100)

# Join the two DataFrames
df.join(df2, on='category', how='inner').show()
```

### WithColumn: Add new calculated columns
```python
# Add a new calculated column
df.withColumn('price_after_tax', df['price'] * 1.1).show()
```

## Spark SQL

### Load Data into a DataFrame
```python
# Load the synthetic data into a DataFrame
data_file = '../data/persons.csv'
persons_df = spark.read.csv(data_file, header=True, inferSchema=True)

# Show the schema of the DataFrame
persons_df.printSchema()

# Show the first 5 rows of the DataFrame
persons_df.show(5)
```

### Register the DataFrame as a Temporary Table
```python
# Register the DataFrame as a Temporary Table
persons_df.createOrReplaceTempView('persons')
```

### Perform SQL-like Queries
```python
# Select all rows where age is greater than 25
query = 'SELECT * FROM persons WHERE age > 25'
persons_df_greater_than_25 = spark.sql(query)
persons_df_greater_than_25.show()

# Compute the average salary of persons
query = 'SELECT AVG(salary) AS avg_salary FROM persons'
avg_salary = spark.sql(query)
avg_salary.show()
```

### Managing temporary views
```python
# Check if a temporary view exists
if spark.catalog._jcatalog.tableExists('persons'):
    print('The temporary view persons exists')

# Drop the temporary view
spark.catalog.dropTempView('persons')

# Check if a temporary view exists
if spark.catalog._jcatalog.tableExists('persons'):
    print('The temporary view persons exists')
```

### Sub Queries
```python
# Create two DataFrames
# The first DataFrame contains employee data with columns: id, name
# The second DataFrame contains salary data with columns: id, salary, department
data1 = [(1, 'John'), (2, 'Jane'), (3, 'Alice')]
data2 = [(1, 1000, 'HR'), (2, 1500, 'Engineering'), (3, 1200, 'Marketing')]
columns1 = ['id', 'name']
columns2 = ['id', 'salary', 'department']
df1 = spark.createDataFrame(data1, columns1)
df2 = spark.createDataFrame(data2, columns2)

# Show the first DataFrame
df1.show()

# Show the second DataFrame
df2.show()
```

```python
# Register as temporary views
df1.createOrReplaceTempView('employees')
df2.createOrReplaceTempView('salaries')
```

```python
# Subquery to find employees with salaries above average
query = '''
SELECT e.name, s.salary
FROM employees e
JOIN salaries s
ON e.id = s.id
WHERE s.salary > (SELECT AVG(salary) FROM salaries)
'''
result = spark.sql(query)
result.show()
```