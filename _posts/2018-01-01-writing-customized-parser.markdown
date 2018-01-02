---
layout: post
title:  "Writing a customized Spark SQL Parser"
date:   2018-01-01
categories: jekyll update
---

Recently, I've been working on a stand-alone Spark SQL related project where I needed to support Spatial queries. Luckily, Spark 2.2 added extension points that allow injecting a customized parser, analyzer or optimizer into Spark session. In this blog post, I will walk through adding the following `KNN` join query (you can access the full code [HERE][code])
{% highlight sql %}
select * from table1 knn join table2 using POINT (x2, y2) knnPred (POINT (x1, y1)
{% endhighlight %}

First, let's take a look at how the extension looks like (you may also check the examples in [SparkSessionExtensionSuite][SparkSessionExtensionSuite]):

{% highlight scala %}
lazy val spark = {
  val conf = new SparkConf().setAppName("example").setMaster("local[1]")
  type ExtensionsBuilder = SparkSessionExtensions => Unit
  def create(builder: ExtensionsBuilder): ExtensionsBuilder = builder
  val extension = create { extensions =>
     extensions.injectParser((_, _) => MyCatalystSqlParser)
  }
  val session = SparkSession.builder().config(conf).withExtensions(extension).getOrCreate()
  session.sparkContext.setLogLevel("ERROR")
  session
}
{% endhighlight %}

The key point is to create an object similar to [CatalystSqlParser][CatalystSqlParser] by isolating then modifying the code in [ParseDriver][ParseDriver] and [AstBuilder][AstBuilder]. Also, we need to update then recompile [SqlBase.g4][SqlBase] grammar.

# The Grammar
Spark SQL uses [ANTLR4][ANTLR] to generate its SQL parser. I recommend skimming through the first four chapters of [The Definitive ANTLR 4 Reference][ANTLR-book] or any equivalent material to understand ANTLR grammar file and the generated parser files. 

Starting with a blank `Scala` project, I copied Spark SQL's grammar file [SqlBase.g4][SqlBase]. After that, I added the new keywords (KNN, POINT, and PREDKNN), updated `joinType` and `joinCriteria` I also defined a new rule named `spatialpredicated` to get my KNN query through the parser. After compiling `SqlBase.g4` I got the updated Parser files. Refer to this [script][script] to compile and post process the grammar file.

# The AstBuilder

The [AstBuilder][AstBuilder] in Spark SQL, processes the ANTLR ParseTree to obtain a Logical Plan. I copied this file into my project and renamed it as [MyAstBuilder][MyAstBuilder]. I only needed to update [withJoinRelations][withJoinRelations] method to handle the KNN join type and implement [visitSpatialpredicated][visitSpatialpredicated] for the [spatialpredicated][spatialpredicated] rule I added to the grammar file. Furthermore, I defined an operator named [SpatialJoin][SpatialJoin] and a dummy prdict to model [PredKnn][predknn].

# A Small Test
Finally, inside the main, I print the optimized plan for the KNN join query.

{% highlight scala %}
System.out.println(spark.sql("select * from table1 knn join table2 using POINT (x2, y2) knnPred (POINT (x1, y1), 5)").queryExecution.optimizedPlan)

SpatialJoin KNNJoin, PredKnn (POINT(x2#8,y2#9), POINT(x1#1,y1#2), 5)
:- Relation[id1#0,x1#1,y1#2] csv
+- Relation[id2#7,x2#8,y2#9] csv
{% endhighlight %}

# Lessons
Here's a few lessons I learned while working on customizing Spark SQL parser:

- The comment `#` in the grammar file means parser generates a visitor method for this rule.
- It's very important to understand the `Expression` class in Spark SQL as it used to model various types of nodes in the query plan.
- Pay attention to Spark SQL package hierarchy. For instance, [MyAstBuilder][MyAstBuilder] should be defined under `org.apache.spark.sql.catalyst.parser` in order to avoid dependency errors.

# Last Word
This blog post is only about customizing Spark SQL's parser. Readers who are interested in implementing various spatial queries in Spark SQL may check [Simba][Simba]. For customizing Spark SQL optimizer you may check [Sunitha's blog][optimizer] post.


[SparkSessionExtensionSuite]: https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/SparkSessionExtensionSuite.scala
[CatalystSqlParser]: https://github.com/apache/spark/blob/3099c574c56cab86c3fcf759864f89151643f837/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/ParseDriver.scala#L130
[ParseDriver]: https://github.com/apache/spark/blob/3099c574c56cab86c3fcf759864f89151643f837/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/ParseDriver.scala
[AstBuilder]: https://github.com/apache/spark/blob/3099c574c56cab86c3fcf759864f89151643f837/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/AstBuilder.scala
[SqlBase]: https://github.com/apache/spark/blob/3099c574c56cab86c3fcf759864f89151643f837/sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4
[ANTLR]: http://www.antlr.org/
[ANTLR-book]: https://pragprog.com/book/tpantlr2/the-definitive-antlr-4-reference
[code]: https://github.com/rtahboub/spark-sql-customized-parser
[MySqlBase]: https://github.com/rtahboub/spark-sql-customized-parser/blob/master/src/main/scala/org/apache/spark/sql/catalyst/parser/SqlBase.g4
[script]: https://github.com/rtahboub/spark-sql-customized-parser/blob/master/process_grammar.sh
[AstBuilder]: https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/AstBuilder.scala
[MyAstBuilder]: https://github.com/rtahboub/spark-sql-customized-parser/blob/master/src/main/scala/org/apache/spark/sql/catalyst/parser/MyAstBuilder.scala
[withJoinRelations]: https://github.com/rtahboub/spark-sql-customized-parser/blob/master/src/main/scala/org/apache/spark/sql/catalyst/parser/MyAstBuilder.scala#L610
[visitSpatialpredicated]: https://github.com/rtahboub/spark-sql-customized-parser/blob/master/src/main/scala/org/apache/spark/sql/catalyst/parser/MyAstBuilder.scala#L994
[spatialpredicated]: https://github.com/rtahboub/spark-sql-customized-parser/blob/master/src/main/scala/org/apache/spark/sql/catalyst/parser/SqlBase.g4#L534
[SpatialJoin]: https://github.com/rtahboub/spark-sql-customized-parser/blob/master/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/basicLogicalOperators.scala#L14
[predknn]: https://github.com/rtahboub/spark-sql-customized-parser/blob/master/src/main/scala/org/apache/spark/sql/catalyst/expressions/preds.scala#L19
[Simba]: https://github.com/InitialDLab/Simba
[optimizer]: https://developer.ibm.com/code/2017/11/30/learn-extension-points-apache-spark-extend-spark-catalyst-optimizer/