(ns sparkling.sql.core
  (:import [org.apache.spark.sql DataFrame SQLContext]
      [org.apache.spark.api.java JavaSparkContext] ))

(defn sql-context
  "Returns an SQL context by wrapping a JavaSparkContext object "
  ([^JavaSparkContext sc]
  (SQLContext. sc)))

(defn create-dataframe
  "Returns a DataFrame "
  [sqc rdd struct-type]
  (.createDataFrame sqc rdd struct-type))
