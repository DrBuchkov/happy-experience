(ns app.core
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [potpuri.core :as ptpr])
  (:gen-class))

(def rating->score
  {"1" -2
   "2" -1
   "3" 1
   "4" 2
   "5" 3})

(defn read-data [reader]
  (let [csv-data (doall (csv/read-csv reader))
        keywords (map keyword (-> csv-data
                                  first))
        csv-data (rest csv-data)]
    (->> csv-data
         (map #(zipmap keywords %)))))

(defn calculate-person-experience [experiences]
  (transduce (map :Rating) + experiences))

(defn calculate-experience [data task-id]
  (->> data
       #_(transduce (comp (filter (ptpr/where-fn {:TaskID task-id}))
                          (map #(update % :Rating rating->score))
                          (map #(select-keys % [:PersonID :Rating :Date])))
                    conj)
       (filter (ptpr/where-fn {:TaskID task-id}))
       (map #(update % :Rating rating->score))
       (map #(select-keys % [:PersonID :Rating :Date]))
       (group-by :PersonID)
       (ptpr/map-vals calculate-person-experience)
       (sort-by second >)))

(defn print-scores [scores]
  (doseq [[person-id score] scores]
    (println (str person-id ";" score))))


(comment
  (def reader (io/reader "small-example.csv"))

  (def data (read-data reader))

  (def task-id "6156dc9dc1f742f8e11aa14d")

  (def person-scores (calculate-experience data task-id))

  )

(defn -main
  [& args]
  (let [[filename task-id] args]
    (with-open [reader (io/reader filename)]
      (let [data (read-data reader)
            scores (calculate-experience data task-id)]
        (print-scores scores)))))