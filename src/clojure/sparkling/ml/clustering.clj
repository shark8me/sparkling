(ns sparkling.ml.clustering
  (:require [sparkling.core :as s]
            [sparkling.ml.core :as mlc]
            [sparkling.destructuring :as s-de]
            [clojure.data.generators :as cdg]
            [sparkling.conf :as conf])
  (:import [org.apache.spark.ml.clustering KMeans KMeansModel LDA LDAModel DistributedLDAModel LocalLDAModel]
           [org.apache.spark.sql.types StructType DataType DataTypes Metadata StructField]
           [org.apache.spark.mllib.linalg Vectors Vector VectorUDT]
           [org.apache.spark.sql.catalyst.expressions GenericRow]
           [org.apache.spark.mllib.regression LabeledPoint]
           [org.apache.spark.sql RowFactory])
  ;(:gen-class)
  )

(defn kmeans
  "Return a instance of Kmeans clustering.
  arguments are hyperparameters used in training the model.
  When no arguments are supplied, the default values (specified in the scala
  implementation are used."
  ([] (kmeans {}))
  ([{:keys [features-col init-mode
            init-steps k
            max-iter
            prediction-col
            seed tol]}]
   (let [km (KMeans.)]
     (when features-col (.setFeaturesCol km features-col))
     (when init-mode (.setInitMode km init-mode))
     (when init-steps (.setInitSteps km init-steps))
     (when k (.setK km k))
     (when max-iter (.setMaxIter km max-iter))
     (when prediction-col (.setPredictionCol km prediction-col))
     (when seed (.setSeed km seed))
     (when tol (.setTol km tol))
     km)))

(defn lda
  "Return a instance of LDA clustering model.
  arguments are hyperparameters used in training the model.
  When no arguments are supplied, the default values (specified in the scala
  implementation are used."
  ([] (lda {}))
  ([{:keys [checkpoint-interval doc-concentration
            doc-concentration-arr features-col
            k learning-decay
            learning-offset max-iter
            optimize-doc-concentration optimizer
            seed subsampling-rate
            topic-concentration topic-distribution-col ]}]
   (let [ldamod (LDA.)]
     (when checkpoint-interval (.setCheckpointInterval ldamod checkpoint-interval))
     (when doc-concentration (.setDocConcentration ldamod doc-concentration ))
     (when doc-concentration-arr (.setDocConcentration ldamod doc-concentration-arr))
     (when features-col (.setFeaturesCol ldamod features-col))
     (when k (.setK ldamod k))
     (when learning-decay (.setLearningDecay ldamod learning-decay))
     (when learning-offset (.setLearningOffset ldamod learning-offset))
     (when max-iter (.setMaxIter ldamod max-iter))
     (when optimize-doc-concentration (.setOptimizeDocConcentration ldamod optimize-doc-concentration ))
     (when optimizer (.setOptimizer ldamod optimizer))
     (when seed (.setSeed ldamod seed))
     (when subsampling-rate (.setSubsamplingRate ldamod subsampling-rate ))
     (when topic-concentration (.setTopicConcentration ldamod topic-concentration ))
     (when topic-distribution-col (.setTopicDistributionCol ldamod topic-distribution-col))
     ldamod)))

(comment
  (s/with-context
    sc  (-> (conf/spark-conf)
            ;(conf/set-sparkling-registrator)
            (conf/set "spark.kryo.registrationRequired" "false")
            (conf/master "local[*]")
            (conf/app-name "clust-test"))
    (let [sqc (mlc/sql-context sc)
          lst (map #(RowFactory/create (into-array Object [%1 %2])) (range 10) (range 10 20))
          lst2 (for [_ (range 10)]
                (GenericRow. (into-array Vector [(Vectors/dense (double-array [(cdg/double) (cdg/double)]))])))
          lp1 (doseq [_ (range 10)]
                (LabeledPoint. (double 0) (Vectors/dense (double-array [(cdg/double) (cdg/double)]))))
          rdd (s/into-rdd sc lst2)
          struct-type (doto (StructType.)
                        (.add  "name" DataTypes/IntegerType)
                        (.add  "col" DataTypes/IntegerType))
          st2 (StructType. (into-array StructField [(StructField. "features" (VectorUDT.) false (Metadata/empty))]))
      df (.createDataFrame sqc rdd st2)
      mode (.fit (kmeans) df)
          ]
       (.clusterCenters mode)
      #_(s/collect rdd)
      ;lst2
      ))



)


