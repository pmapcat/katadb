(ns katadb.core
  {:doc "1. File based durable Queue, write to it
         2. Read it
         3. Subscribe to it
         4. Discard blocks of it, based on policy"}
  (:gen-class))

;; =================== INFRA/FILE API (layer 0) ==========================


(defn tid
  []
  (.getId (Thread/currentThread)))


(let [plock (Object.)]
  (defn INFO [& args]
    "does logging using lock"
    (locking plock (apply println args))))


(defn -consume-recur
  "reads input line by line, returning lazy seq. Blocks, polling for changes every poll-time"
  [r poll-time]
  (lazy-seq
    (let [next (.readLine r)]
      (cons next
        (do (when-not next (Thread/sleep poll-time))
            (-consume-recur r))))))


(defn consume-file
  [reader poll-time]
  (remove nil? (-consume-recur reader poll-time)))

#_(future
    (doseq [i (consume-file (clojure.java.io/reader "/home/mik/toy.log" 200))]
      (INFO "Thread: " (tid) i)))


(defn third
  [a]
  (first (next (next a))))


(defn file-list
  [fpath]
  (for [i     (.listFiles (clojure.java.io/file fpath))
        :when (.isFile i)]
    (str i)))
#_(file-list "/home/mik/Projects/")


(defn utf8-len
  [string]
  (when string
    (alength (.getBytes string "utf8"))))


(defn encode-fixed-utf8
  "record is a UTF8 encoded fixed length message
   pads with ' ' if not enough,  throws, if larger 
   than RECORD-SIZEB

   fixed length is useful: we can control and define 
   bucket fullness, we can random access, we can use 
   bisection. We use \n because that is how Grep and Emacs
   are fast. Data should be easy to inspect"
  [record sizeb]
  (let [bsize (utf8-len record)]
    (if (> bsize (dec sizeb)) nil
        (str record (clojure.string/join "" (repeat (- sizeb (inc bsize)) " ")) "\n"))))
(assert (= (utf8-len (encode-fixed-utf8 "привіт" RECORD-SIZEB)) RECORD-SIZEB))
(assert (= (utf8-len (encode-fixed-utf8 "" RECORD-SIZEB)) RECORD-SIZEB))
(assert (= (utf8-len (encode-fixed-utf8 (clojure.string/join "" (repeat RECORD-SIZEB "h")) RECORD-SIZEB)) nil))


(defn append-file!
  [fpath encoding valseq]
  (with-open [w! (clojure.java.io/writer fpath :append true :encoding encoding)]
    (doseq [v valseq]
      (.write w! v))
    (.flush w!)))
#_(append-file! "/home/mik/toy.log" "utf8" ["привіт" "jshf"])


(defn file-size
  [fpath]
  (.length (clojure.java.io/file fpath)))


;; ===================== LOG FILE (layer 1) ======================

;; ;; probably nice abstraction ?? 
;; (defprotocol LogFile
;;   "logfile is an endless log file with capability to be truncated on demand"
;;   (defmethod open     [fpath])
;;   (defmethod write    [record] "Will write log sequence, determines ")
;;   (defmethod seq      [] "Returns lazyseq that will read until the log file, will block for new records ")
;;   (defmethod truncate []))

(def MAX-OFFSET Long/MAX_VALUE)
(def POLL-TIME 1000)
(def RECORD-SIZEB 100)
(def BUCKET-SIZER 10000)
(def BUCKET-LIMITB (* RECORD-SIZEB BUCKET-SIZER))
(def BASE-CODING "utf8")
(def FILES-RE #"^(\d+)_(\d+)(\.[a-z]+)$")


(defn split-f
  [f]
  (let [[_ l r ext] (re-matches FILES-RE f)]
    [_ (Integer/parseInt l) (Integer/parseInt r) ext]))


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


(defn split-files
  [files]
  (map FILES-RE files))


(defn log-filelist
  [fpath]
  (file-)
  )


(defn get-head
  [fpath]
  (file-list fpath))

(defn full?
  [fpath]
  (< (fsize fpath) BUCKET-LIMITB))



(defn get-bucket
  [dbpath]
  (or (last (filter (comp not full?) file-list))
    
    )
  )


(defn log-write!
  [fpath valseq]
  (first (not-fulls (list-files "/home/mik/demodb/")))
    
  )
#_(db-write! "/home/mik/demodb/" "hello test")



(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
