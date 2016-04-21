(ns sparkling.sql.dataframe
  (:require [sparkling.core :as s]
            [sparkling.ml.core :as mlc]
            [sparkling.destructuring :as s-de]
            [sparkling.conf :as conf])
  (:import [org.apache.spark.ml.clustering KMeans KMeansModel LDA LDAModel DistributedLDAModel LocalLDAModel]
           [org.apache.spark.sql.types StructType DataType DataTypes Metadata StructField]
           [org.apache.spark.mllib.linalg Vectors Vector VectorUDT]
           [org.apache.spark.sql.catalyst.expressions GenericRow]
           [org.apache.spark.mllib.regression LabeledPoint]
           [org.apache.spark.sql RowFactory]))

(defn create-ds-row
  "Takes a seq, where each entry is a double array. Returns a lazy sequence of Row objects."
  [rawdata]
  (for [i rawdata]
     (GenericRow. (into-array Vector [(Vectors/dense (double-array i))]))))

(defn vector-structtype
  "returns a StructType that uses dense vectors as the only type "
  ([field-name] (vector-structtype field-name false (Metadata/empty)))
  ([field-name nullable metadata]
  (StructType. (into-array StructField [(StructField. field-name (VectorUDT.) nullable metadata)]))))
