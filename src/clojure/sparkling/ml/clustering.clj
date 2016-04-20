(ns sparkling.ml.clustering
  (:import [org.apache.spark.ml.clustering KMeans KMeansModel LDA LDAModel DistributedLDAModel LocalLDAModel]))

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
