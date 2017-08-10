(ns sparkling.rdd.bench
  (:require [sparkling.api :as s]
            [citius.core :as c]
            [criterium.core :as b]
            [sparkling.core :as spark]
            [sparkling.conf :as conf]
            [clojure.test :refer :all]))

(b/execute-expr 100 #(+ 2 2))
(b/with-progress-reporting
  (b/quick-bench (+ 2 2) 
                 :samples 10
                 :warmup-jit-period 1000))
(def data
  ["This is a firest line"  
   "Testing spark" 
   "and sparkling" 
   "Happy hacking!"])

(def numxs (vec (range 1000)))
(def sc (spark/spark-context (-> (conf/spark-conf)
                                 (conf/app-name "sparkling-test") 
                                 (conf/master "local"))))

(spark/stop sc)
(defn run1 []
  (let  [lines-rdd  (spark/into-rdd sc data)]
    (count (spark/collect (spark/filter #(.contains % "spark") lines-rdd)))))
(defn run2[]
(let  [lines-rdd  (spark/into-rdd sc numxs)]
(count (spark/collect (spark/filter even? lines-rdd)))))
(time (+ 2 2))
(time (b/execute-expr 10 run2))
(b/with-progress-reporting
  (b/quick-bench (run1)
                 :warmup-jit-period 10
                 :samples 10))
(defn context-within
  [in-data]
 (spark/with-context sc  
  (-> (conf/spark-conf)
      (conf/app-name "sparkling-test") 
      (conf/master "local"))
   (let  [lines-rdd  (spark/into-rdd sc in-data)] 
     (count (spark/collect (spark/filter #(.contains % "spark") lines-rdd))))))

(time (b/execute-expr 10 (partial context-within data) ))
(b/bench (context-within data))
(defn context-external
  [in-data]
 (spark/with-context sc  
  (-> (conf/spark-conf)
      (conf/app-name "sparkling-test") 
      (conf/master "local"))
   (let  [lines-rdd  (spark/into-rdd sc in-data)
          fnx (fn [_] (spark/collect (spark/filter #(.contains % "spark") lines-rdd)))]
     (->> (mapv fnx (range 10))
          (mapv count)
          first))))

(defn context-ext
  [in-data]
 (spark/with-context sc  
  (-> (conf/spark-conf)
      (conf/app-name "sparkling-test") 
      (conf/master "local"))
   (let  [fnx (fn [_] (spark/collect 
                        (spark/filter #(.contains % "spark") 
                          (spark/into-rdd sc in-data))))]
     (->> (mapv fnx (range 10))
          (mapv count)
          first))))

(def descriptions
  ["single run within context"
   "10 runs, filter, collect repeat"
   "10 runs, into-rdd, filter, collect repeat"])

(clojure.test/use-fixtures 
  :once (c/make-bench-wrapper descriptions
         {:chart-title "Comparison of counting methods"
          :chart-filename (format "confmat-counting-%s.png" c/clojure-version-str)}))
;^:benchmarking
(deftest  test-counts
  (testing "abd"
   (c/compare-perf "instances" 
                   (is (= 2 (context-within data)))
                   (is (= 2 (context-external data)))
                   (is (= 2 (context-ext data))))))
(c/with-bench-context ["1"] 
  {:chart-title "bench"
   :chart-filename (format "bench-simple-clj-%s.png" c/clojure-version-str)}
  (c/compare-perf "instances"
                  (run1) 
                  ))
(comment
  (c/with-bench-context descriptions 
    {:chart-title "bench"
     :chart-filename (format "bench-simple-clj-%s.png" c/clojure-version-str)}
  (c/compare-perf "instances"
                  (context-within data)
                  (context-external data)
                  (context-ext data)))
  )
