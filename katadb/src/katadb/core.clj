(ns katadb.core
  {:doc "1. File based durable Queue, write to it
         2. Read it
         3. Subscribe to it
         4. Discard blocks of it, based on policy"}
  (:require [katadb.const :as c])
  (:gen-class))



(def MAX-OFFSET Long/MAX_VALUE)
(def POLL-TIME 1000)
(def RECORD-SIZEB 1024)
(def BUCKET-SIZER 10000)
(def BASE-CODING "utf8")
(def FILES-RE #"^(\d+)_(\d+)(\.[a-z]+)$")

(defprotocol LogFileWriter
  "logfile is an endless log file with capability to be truncated if needed"
  (defmethod make     [fpath])
  (defmethod write    [record] "Will write log sequence, determines ")
  (defmethod seq      [] "Returns lazyseq that will read until the log file, will block for new records ")
  (defmethod truncate []))


(defrecord FSLogFile
    LogFile
  (make [fpath]
    "initialize under current folder, seeking last buffer")
  
  (write [record]
    "get current bucket and write to it, possibly moving target, if the bucket is full")

  (seq []
    "from the starting bucket to the current, do the read line, 
     when nothing to read, block for the next record
     when nothing to read, bucket is full, move forward 
     and block for that data")
  
  (truncate []
    "will delete records that are older than N offset, 
     Dangerous, what if we are still reading while consuming data?
     will need a master truncator and some sort of state control"))


(defrecord EphLogFile
    LogFile
  )





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


(defn subscribe
  "subscribes to a file"
  [fpath])


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
