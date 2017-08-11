(ns sparkling.rdd.bench
  (:require [sparkling.api :as s]
           ; [citius.core :as c]
            [criterium.core :as b]
            [sparkling.core :as spark]
            [sparkling.conf :as conf]
            [clojure.test :refer :all]))

(def data
  ["This is a firest line"  
   "Testing spark" 
   "and sparkling" 
   "Happy hacking!"])

(def numxs (vec (range 1000)))
(def sc (spark/spark-context (-> (conf/spark-conf)
                                 (conf/app-name "sparkling-test") 
                                 (conf/master "local"))))

(defn wtime
  "returns average time in msecs for 10 runs"
  [f]
  (time (-> (b/execute-expr 10 f)
            first
            ;;msec per run for 10 runs
            (/ 1E7))))
;;(spark/stop sc)
(defn run1 []
  (let  [lines-rdd  (spark/into-rdd sc data)]
    (count (spark/collect (spark/filter #(.contains % "spark") lines-rdd)))))

(wtime run1)
;;47
;;39
;;41

(b/quick-bench run1)

(comment
  Evaluation count : 102092022 in 6 samples of 17015337 calls.
  Execution time mean : 3.679218 ns
  Execution time std-deviation : 0.253643 ns
  Execution time lower quantile : 3.485542 ns ( 2.5%)
  Execution time upper quantile : 4.100893 ns (97.5%)
  Overhead used : 2.059620 ns

  Found 1 outliers in 6 samples (16.6667 %)
	low-severe	 1 (16.6667 %)
  Variance from outliers : 15.1446 % Variance is moderately inflated by outliers
  )

(defn run2 []
  (let  [lines-rdd  (spark/into-rdd sc data)
         _ (spark/storage-level! (spark/STORAGE-LEVELS :memory-only) lines-rdd)]
    (count (spark/collect (spark/filter #(.contains % "spark") lines-rdd)))))

(wtime run2)
;;56
;;38
;;40

(b/quick-bench run2)
(comment

  Evaluation count : 103434372 in 6 samples of 17239062 calls.
  Execution time mean : 3.826064 ns
  Execution time std-deviation : 0.305970 ns
  Execution time lower quantile : 3.452323 ns ( 2.5%)
  Execution time upper quantile : 4.313033 ns (97.5%)
  Overhead used : 2.059620 ns

  Found 2 outliers in 6 samples (33.3333 %)
	low-severe	 1 (16.6667 %)
	low-mild	 1 (16.6667 %)
  Variance from outliers : 15.5355 % Variance is moderately inflated by outliers
  )

;;takes too long for jit warmup
(b/with-progress-reporting
  (b/quick-benchmark* run1 {:verbose true
                            :warmup-jit-period 100}))

(def saved-rdd
  (let  [lines-rdd  (spark/into-rdd sc data)]
    (spark/storage-level! (spark/STORAGE-LEVELS :memory-only) lines-rdd)))

(defn run3 []
  (count (spark/collect (spark/filter #(.contains % "spark") saved-rdd))))

(wtime run3)
;;39
;;38
;;33

(b/quick-bench run3)
(comment

  Evaluation count : 85257906 in 6 samples of 14209651 calls.
  Execution time mean : 5.431549 ns
  Execution time std-deviation : 0.650052 ns
  Execution time lower quantile : 4.967727 ns ( 2.5%)
  Execution time upper quantile : 6.454783 ns (97.5%)
  Overhead used : 2.059620 ns

  Found 1 outliers in 6 samples (16.6667 %)
	low-severe	 1 (16.6667 %)
  Variance from outliers : 31.3132 % Variance is moderately inflated by outliers
  )

(def saved-rdd2
  (let  [lines-rdd  (spark/into-rdd sc data)
         _ (spark/collect lines-rdd)]
    lines-rdd))

(defn run4 []
  (count (spark/collect (spark/filter #(.contains % "spark") saved-rdd2))))

(wtime run4)
;;33
;;30
;;34
(b/quick-bench run4)

(comment

  Evaluation count : 136910370 in 6 samples of 22818395 calls.
  Execution time mean : 2.259861 ns
  Execution time std-deviation : 0.213861 ns
  Execution time lower quantile : 2.070495 ns ( 2.5%)
  Execution time upper quantile : 2.597211 ns (97.5%)
  Overhead used : 2.059620 ns

  Found 1 outliers in 6 samples (16.6667 %)
  low-severe	 1 (16.6667 %)
  Variance from outliers : 30.1024 % Variance is moderately inflated by outliers
  )







