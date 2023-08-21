from pyspark.sql import SparkSession
from pyspark.sql.functions import isnull, when, count, col
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
# Evaluate our model
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder.appName('Titanic Data').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = (spark.read.format("csv").option('header', 'true').load("/opt/spark-data/train.csv"))
df.show(5)

dataset = df.select(col('Survived').cast('float'),
                    col('Pclass').cast('float'),
                    col('Sex'),
                    col('Age').cast('float'),
                    col('Fare').cast('float'),
                    col('Embarked'),
                    )

dataset.show()
#dataset.select([count(when(isnull(c), c)).alias(c) for c in dataset.columns]).show()

dataset = dataset.replace('?', None).dropna(how='any')

dataset = StringIndexer(
    inputCol='Sex',
    outputCol='Gender',
    handleInvalid='keep').fit(dataset).transform(dataset)

dataset = StringIndexer(
    inputCol='Embarked',
    outputCol='Boarded',
    handleInvalid='keep').fit(dataset).transform(dataset)
#dataset.show()


#drop column
dataset = dataset.drop('Sex')
dataset = dataset.drop('Embarked')
#dataset.show()

required_features = ['Pclass','Age','Fare','Gender','Boarded']

assembler = VectorAssembler(inputCols=required_features, outputCol='features')
transformed_data = assembler.transform(dataset)
training_data = transformed_data

#Load data test set ================================================================
df_test = (spark.read.format("csv").option('header', 'true').load("/opt/spark-data/test.csv"))

dataset_test = df_test.select(
                    col('Pclass').cast('float'),
                    col('Sex'),
                    col('Age').cast('float'),
                    col('Fare').cast('float'),
                    col('Embarked')
                    )

dataset_test = dataset_test.replace('?', None).dropna(how='any')

dataset_test = StringIndexer(
    inputCol='Sex',
    outputCol='Gender',
    handleInvalid='keep').fit(dataset_test).transform(dataset_test)

dataset_test = StringIndexer(
    inputCol='Embarked',
    outputCol='Boarded',
    handleInvalid='keep').fit(dataset_test).transform(dataset_test)

dataset_test = dataset_test.drop('Sex')
dataset_test = dataset_test.drop('Embarked')

required_features = ['Pclass','Age','Fare','Gender','Boarded']

assembler = VectorAssembler(inputCols=required_features, outputCol='features')
transformed_data_test = assembler.transform(dataset_test)
test_data = transformed_data_test



# =============== Build and test model ==================================
#(training_data, test_data) = transformed_data.randomSplit([0.8,0.2])

rf = RandomForestClassifier(labelCol='Survived', featuresCol='features', maxDepth=5)
model =rf.fit(training_data)
predictions = model.transform(test_data)
predictions.show()

evaluator = MulticlassClassificationEvaluator(
    labelCol='Survived',
    predictionCol='prediction',
    metricName='accuracy'
)
accuracy = evaluator.evaluate(predictions)
print('Test Accuracy =',accuracy)
