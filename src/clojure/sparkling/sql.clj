(ns sparkling.sql
  "封装了部分 spark sql api ，使之更符合 clojure 的风格。
参数风格参考 sparkling，数据集尽量放在最后一个参数。read json 和 wrte json 与
sparkling 的 text-file 和 save-as-text-file 一致。 "
  (:require [sparkling.function :as func])
  (:import [org.apache.spark.sql SQLContext]
           [org.apache.spark.sql functions]
           [com.google.common.collect ImmutableMap]))

(defn sql-context
  "create new SQLContext by spark context"
  [sc]
  (SQLContext. sc))

(defn sql
  "give sql script then return data frame"
  [code context]
  (.sql context code))

(defn register-temp-table
  "regist data frame as a tmp table named table-name and return data frame"
  [table-name data-frame]
  (.registerTempTable data-frame table-name)
  data-frame)

(defn select
  "封装 DataFrame 的 select 方法。"
  [cols data-frame]
  (.select data-frame
           (into-array cols)))

(defn cols
  "create a vector for columns by given names"
  [names data-frame]
    (vec (map #(.col data-frame %) names)))

(defn selects
  "call select by given names"
  [columns data-frame]
  (.select data-frame
           (into-array (cols columns data-frame))))

(defn select-expr
  "call select expr by given column expressions"
  [expr data-frame]
  (.selectExpr data-frame
               (into-array expr)))

(defn where
  "call where by "
  [expression data-frame]
  (.where data-frame expression))

(defn join
  "封装 DataFrame 的 join 方法。"
  [expression other data-frame]
  (.join data-frame expression))

(defn join-on
  "根据给定的字段文本构造两个 data frame 的 inner join 。"
  [column-name other data-frame]
  (.join data-frame other
         (.equalTo (.col data-frame column-name) (.col other column-name))))

(defn group-by
  "封装 DataFrame 的 groupBy 操作"
  [cols data-frame]
  (.groupBy data-frame (into-array cols)))

(defn group-by-cols
  "根据给定的字段文本列表构造 group by 操作并调用"
  [columns data-frame]
  (.groupBy data-frame (into-array (cols columns data-frame))))

(defn count
  "grouped data count"
  [cols grouped-data]
  (.max grouped-data (into-array cols)))

(defn max
  "grouped data max"
  [cols grouped-data]
  (.max grouped-data (into-array cols)))

(defn min
  "grouped min"
  [cols grouped-data]
  (.min grouped-data (into-array cols)))

(defn sum
  "grouped sum"
  [cols grouped-data]
  (.min grouped-data (into-array cols)))

(defn agg
  "dataset agg"
  [cols dataset]
  (.agg dataset (ImmutableMap/copyOf (apply hash-map cols))))

(defn with-column-renamed
  "DataFrame with column renamed."
  [exist-name new-name data-frame]
  (.withColumnRenamed data-frame exist-name new-name))

(defn order-by
  "封装 DataFrame 的 orderBy 操作"
  [cols data-frame]
  (.orderBy data-frame (into-array cols)))

(defn order-by-cols
  "根据给定的字段文本列表构造order by操作并调用"
  [columns data-frame]
  (.orderBy data-frame (into-array (cols columns data-frame))))

(defn register-udf1
  "向 SQLContext 中注册 udf ，这个函数适配 java api udf1 。函数返回传入的 SQLContext。"
  [name func data-type sqlc]
  (-> sqlc
      .udf
      (.register name (func/function func) data-type))
  sqlc)

(defn register-udf2
  "向 SQLContext 中注册 udf ，这个函数适配 java api udf2 。函数返回传入的 SQLContext。"
  [name func data-type sqlc]
  (-> sqlc
      .udf
      (.register name (func/function2 func) data-type))
  sqlc)

(defn json-rdd
  "将 data frame 转为 json 格式的 java rdd"
  [data-frame]
  (-> data-frame
      .toJSON
      .toJavaRDD))

(defn read-json
  "将给定的 json 资源（文件路径或 rdd ）加载为 data-frame"
  [sqlc data-source]
  (-> sqlc
      .read
      (.json data-source)))

(defn write-json
  "将给定的 data-frame 以 json 格式保存到指定路径。"
  [data-source data-frame]
  (-> data-frame
      .write
      (.json data-source)))
