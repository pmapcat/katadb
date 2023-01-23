(ns katadb.core
  {:doc "1. File based durable Queue, write to it
         2. Read it
         3. Subscribe to it
         4. Discard blocks of it, based on policy"}
  (:gen-class))


(def MAX-OFFSET Long/MAX_VALUE)
(def POLL-TIME 1000)
(def RECORD-SIZEB 1024)
(def BUCKET-SIZER 10000)
(def BASE-CODING "utf8")
(def FILES-RE #"^(\d+)_(\d+)(\.[a-z]+)$")

;; ;; probably nice abstraction ?? 
;; (defprotocol LogFile
;;   "logfile is an endless log file with capability to be truncated on demand"
;;   (defmethod open     [fpath])
;;   (defmethod write    [record] "Will write log sequence, determines ")
;;   (defmethod seq      [] "Returns lazyseq that will read until the log file, will block for new records ")
;;   (defmethod truncate []))


(defn tid
  []
  (.getId (Thread/currentThread)))


(let [plock (Object.)]
  (defn INFO [& args]
    "does logging using lock"
    (locking plock (apply println args))))


(defn -consume-recur
  "reads input line by line, returning lazy seq. Blocks, polling for changes every POLL-TIME"
  [r]
  (lazy-seq
    (let [next (.readLine r)]
      (cons next
        (do (when-not next (Thread/sleep POLL-TIME))
            (-consume-recur r))))))


(defn consume-file
  [reader]
  (remove nil? (-consume-recur reader)))


#_(future
    (doseq [i (consume-file (clojure.java.io/reader "/home/mik/toy.log"))]
      (INFO "Thread: " (tid) i)))


(defn third
  [a]
  (first (next (next a))))


(defn split-f
  [f]
  (let [[_ l r ext] (re-matches FILES-RE f)]
    [_ (Integer/parseInt l) (Integer/parseInt r) ext]))


(defn split-files
  [files]
  (map FILES-RE files))


(defn list-files
  [fpath]
  (for [i (.listFiles (clojure.java.io/file fpath))
        :when (.isFile i)]
    (str i)))
#_(list-files "/home/mik/Projects/")


(defn logseq
  ([] (logseq 0))
  ([begin-point]
   (for [i (range begin-point MAX-OFFSET BUCKET-SIZER)]
     (str i "_" (+ i BUCKET-SIZER) ".log"))))
#_(map split-f (take 10 (logseq)))


(defn maxoffset
  "does not guarantee that this offset is the last offset. 
   for examle, if db dies, then the bucket is not full, we
   restart writing making a new bucket for simplicity"
  [files]
  (apply max (map (comp third split-f) files)))


(defn encode-record
  "record is a UTF8 encoded fixed length message
   pads with ' ' if not enough,  throws, if larger 
   than RECORD-SIZEB

   fixed length is useful: we can control and define 
   bucket fullness, we can random access, we can use 
   bisection

   we use \n because that is how Grep and Emacs
   are fast. Data should be easily inspectable "
  [record]
  "asdasdas")



(defn define-bucket
  [fpath]
  
  )

(defn write!
  [fpath val]
  (with-open [w! (clojure.java.io/writer fpath
                   :append true
                   :encoding BASE-CODING)]
    (.write w! val)
    (.flush w!)))


(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
