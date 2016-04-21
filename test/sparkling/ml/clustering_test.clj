(ns sparkling.ml.clustering-test
  (:require [sparkling.core :as s]
            [sparkling.ml.core :as mlc]
            [sparkling.ml.clustering :as mlct]
            [sparkling.sql.core :as sqc]
            [sparkling.sql.dataframe :as df]
            [sparkling.destructuring :as s-de]
            [clojure.data.generators :as cdg]
            [clojure.test :as t]
            [sparkling.conf :as conf])
  (:import [org.apache.spark.ml.clustering KMeans KMeansModel LDA LDAModel DistributedLDAModel LocalLDAModel]
           [org.apache.spark.sql.types StructType DataType DataTypes Metadata StructField]
           [org.apache.spark.mllib.linalg Vectors Vector VectorUDT]
           [org.apache.spark.sql.catalyst.expressions GenericRow]
           [org.apache.spark.mllib.regression LabeledPoint]
           [org.apache.spark.sql RowFactory]))

(def testconf (-> (conf/spark-conf)
                  (conf/set-sparkling-registrator)
                  (conf/set "spark.kryo.registrationRequired" "false")
                  (conf/master "local[*]")
                  (conf/app-name "clust-test")))

(defn take-rand-doub
  [k]
  (take k (repeatedly cdg/double)))

(defn getxy-data
  [m n]
  (for [i (range m)]
    (take-rand-doub n)))

(t/deftest kmeans-test
  (let [ rawdata (getxy-data 500 2)
         k 5
         res
         (s/with-context
           sc testconf
           (let [sqlc (sqc/sql-context sc)
                 lst (df/create-ds-row rawdata)
                 rdd (s/into-rdd sc lst)
                 st (df/vector-structtype "features")
                 dframe (sqc/create-dataframe sqlc rdd st)
                 mode (mlc/fit (mlct/kmeans {:k k}) dframe)]
             (s/collect (mlc/transform mode dframe))))]
   (t/is (= k (count (reduce into #{} (map #(hash-set (.get % 1)) res)))))))
