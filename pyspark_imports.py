# Standard PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# SparkContext and configuration (if needed)
from pyspark import SparkConf, SparkContext

# For MLlib (if you're doing machine learning)
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    VectorAssembler, StringIndexer, OneHotEncoder, StandardScaler
)
from pyspark.ml.classification import (
    LogisticRegression, RandomForestClassifier, GBTClassifier
)
from pyspark.ml.regression import (
    LinearRegression, RandomForestRegressor, GBTRegressor
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator, MulticlassClassificationEvaluator, RegressionEvaluator
)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MyPySparkApp") \
    .getOrCreate()

# Optional: SparkContext (can be useful for lower-level RDD APIs)
sc = spark.sparkContext

# Stop the Spark session and context
spark.stop()