(ns metabase.driver.databricks-sql
    (:require [clojure.java.jdbc :as jdbc]
      [clojure.string :as str]
      [buddy.core.codecs :as codecs]
      [java-time :as t]
      [honey.sql :as sql]
      [medley.core :as m]
      [metabase.driver :as driver]
      [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
      [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
      [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
      [metabase.driver.sql.query-processor :as sql.qp]
      [metabase.driver.sql.util.unprepare :as unprepare]
      [metabase.driver.sql.util :as sql.u]
      [metabase.mbql.util :as mbql.u]
      [metabase.util.date-2 :as u.date]
      [metabase.util.honey-sql-2 :as h2x]
      [metabase.query-processor.util :as qp.util])
    (:import [java.sql Connection ResultSet Types]
             [java.time LocalDate OffsetDateTime ZonedDateTime]))

(set! *warn-on-reflection* true)

(driver/register! :databricks-sql, :parent :sql-jdbc)

(defmethod sql-jdbc.conn/connection-details->spec :databricks-sql
           [_ {:keys [host http-path token schema catalog current-schema]}]
           {:classname     "com.databricks.client.jdbc.Driver"
            :subprotocol   "databricks"
            :subname       (str "//" host ":443/" schema)
            :transportMode "http"
            :ssl           1
            :AuthMech      3
            :httpPath      http-path
            :uid           "token"
            :pwd           token
            :ConnCatalog   catalog
            ;;    :ConnSchema       schema
            })

(defmethod sql-jdbc.conn/data-warehouse-connection-pool-properties :databricks-sql
           [driver database]
           (merge
             ((get-method sql-jdbc.conn/data-warehouse-connection-pool-properties :sql-jdbc) driver database)
             {"preferredTestQuery" "SELECT 1"}))

(defmethod sql-jdbc.sync/database-type->base-type :databricks-sql
           [_ database-type]
           (condp re-matches (str/lower-case (name database-type))
                  #"boolean" :type/Boolean
                  #"tinyint" :type/Integer
                  #"smallint" :type/Integer
                  #"int" :type/Integer
                  #"bigint" :type/BigInteger
                  #"float" :type/Float
                  #"double" :type/Float
                  #"double precision" :type/Double
                  #"decimal.*" :type/Decimal
                  #"char.*" :type/Text
                  #"varchar.*" :type/Text
                  #"string.*" :type/Text
                  #"binary*" :type/*
                  #"date" :type/Date
                  #"time" :type/Time
                  #"timestamp" :type/DateTime
                  #"interval" :type/*
                  #"array.*" :type/Array
                  #"map" :type/Dictionary
                  #".*" :type/*))

(defmethod sql.qp/honey-sql-version :databricks-sql
           [_driver]
           2)
(defn- dash-to-underscore [s]
       (when s
             (str/replace s #"-" "_")))

(defn- valid-describe-table-row? [{:keys [col_name data_type]}]
       (every? (every-pred (complement str/blank?)
                           (complement #(str/starts-with? % "#")))
               [col_name data_type]))

(defmethod driver/describe-table :databricks-sql
  [driver database {table-name :name, schema :schema}]
  {:name   table-name
   :schema schema
   :fields
   (with-open [conn (jdbc/get-connection (sql-jdbc.conn/db->pooled-connection-spec database))]
     (let [results (jdbc/query {:connection conn} [(format
                                                    "describe %s"
                                                    (sql.u/quote-name driver :table
                                                                      (dash-to-underscore schema)
                                                                      (dash-to-underscore table-name)))])]
       (set
        (for [[idx {col-name :col_name, data-type :data_type, :as result}] (m/indexed results)
              :while (valid-describe-table-row? result)]
          {:name              col-name
           :database-type     data-type
           :base-type         (sql-jdbc.sync/database-type->base-type :databricks-sql (keyword data-type))
           :database-position idx}))))})

(defmethod driver/describe-database :databricks-sql
           [_ database]
           {:tables
            (with-open [conn (jdbc/get-connection (sql-jdbc.conn/db->pooled-connection-spec database))]
                       (let [schemas (jdbc/query {:connection conn} ["show schemas"])
                             tables-info (atom #{})]
                            (doseq [schema-row schemas]
                                   (let [schema-name (:databasename schema-row)]
                                        (when (and (string? schema-name) (not (empty? schema-name)))
                                              (let [tables (jdbc/query {:connection conn} [(str "show tables in " schema-name)])]
                                                   (doseq [table-row tables]
                                                          (let [tablename (:tablename table-row)]
                                                               (when (and (string? tablename) (not (empty? tablename)))
                                                                     (swap! tables-info conj {:name tablename :schema schema-name}))))))))
                            (deref tables-info)))})


(def ^:dynamic *param-splice-style*
  "How we should splice params into SQL (i.e. 'unprepare' the SQL). Either `:friendly` (the default) or `:paranoid`.
  `:friendly` makes a best-effort attempt to escape strings and generate SQL that is nice to look at, but should not
  be considered safe against all SQL injection -- use this for 'convert to SQL' functionality. `:paranoid` hex-encodes
  strings so SQL injection is impossible; this isn't nice to look at, so use this for actually running a query."
  :friendly)

(defmethod driver/execute-reducible-query :databricks-sql
           [driver {{sql :query, :keys [params], :as inner-query} :native, :as outer-query} context respond]
           (let [inner-query (-> (assoc inner-query
                                        :remark (qp.util/query->remark :databricks-sql outer-query)
                                        :query (if (seq params)
                                                 (binding [*param-splice-style* :paranoid]
                                                          (unprepare/unprepare driver (cons sql params)))
                                                 sql)
                                        :max-rows (mbql.u/query->max-rows-limit outer-query))
                                 (dissoc :params))
                 query (assoc outer-query :native inner-query)]
                ((get-method driver/execute-reducible-query :sql-jdbc) driver query context respond)))


(defmethod sql-jdbc.execute/connection-with-timezone :databricks-sql
           [driver database _timezone-id]
           (let [conn (.getConnection (sql-jdbc.execute/datasource-with-diagnostic-info! driver database))]
                (try
                  (.setTransactionIsolation conn Connection/TRANSACTION_READ_UNCOMMITTED)
                  conn
                  (catch Throwable e
                    (.close conn)
                    (throw e)))))

(defmethod sql-jdbc.execute/prepared-statement :databricks-sql
           [driver ^Connection conn ^String sql params]
           (let [stmt (.prepareStatement conn sql
                                         ResultSet/TYPE_FORWARD_ONLY
                                         ResultSet/CONCUR_READ_ONLY)]
                (try
                  (.setFetchDirection stmt ResultSet/FETCH_FORWARD)
                  (sql-jdbc.execute/set-parameters! driver stmt params)
                  stmt
                  (catch Throwable e
                    (.close stmt)
                    (throw e)))))



(defmethod sql-jdbc.execute/statement-supported? :databricks-sql [_] false)

(doseq [[feature supported?] {:basic-aggregations              true
                              :binning                         true
                              :expression-aggregations         true
                              :expressions                     true
                              :native-parameters               true
                              :nested-queries                  true
                              :standard-deviation-aggregations true
                              :test/jvm-timezone-setting       false}]
       (defmethod driver/database-supports? [:databricks-sql feature] [_driver _feature _db] supported?))


(when-not (get (methods driver/database-supports?) [:databricks-sql :foreign-keys])
          (defmethod driver/database-supports? [:databricks-sql :foreign-keys] [_driver _feature _db] true))

(defmethod sql.qp/quote-style :databricks-sql
           [_driver]
           :mysql)


;; Date Utils

(defmethod sql.qp/current-datetime-honeysql-form :databricks-sql
  [_]
  (h2x/with-database-type-info :%now "timestamp"))

(defmethod sql.qp/unix-timestamp->honeysql [:databricks-sql :seconds]
  [_ _ expr]
  (h2x/->timestamp [:from_unixtime expr]))

(defn- date-format [format-str expr]
  [:date_format expr (h2x/literal format-str)])

(defn- str-to-date [format-str expr]
  (h2x/->timestamp [:from_unixtime [:unix_timestamp expr (h2x/literal format-str)]]))

(defn- trunc-with-format [format-str expr]
  (str-to-date format-str (date-format format-str expr)))

(defmethod sql.qp/date [:databricks-sql :default]         [_ _ expr] (h2x/->timestamp expr))
(defmethod sql.qp/date [:databricks-sql :minute]          [_ _ expr] (trunc-with-format "yyyy-MM-dd HH:mm" (h2x/->timestamp expr)))
(defmethod sql.qp/date [:databricks-sql :minute-of-hour]  [_ _ expr] [:minute (h2x/->timestamp expr)])
(defmethod sql.qp/date [:databricks-sql :hour]            [_ _ expr] (trunc-with-format "yyyy-MM-dd HH" (h2x/->timestamp expr)))
(defmethod sql.qp/date [:databricks-sql :hour-of-day]     [_ _ expr] [:hour (h2x/->timestamp expr)])
(defmethod sql.qp/date [:databricks-sql :day]             [_ _ expr] (trunc-with-format "yyyy-MM-dd" (h2x/->timestamp expr)))
(defmethod sql.qp/date [:databricks-sql :day-of-month]    [_ _ expr] [:dayofmonth (h2x/->timestamp expr)])
(defmethod sql.qp/date [:databricks-sql :day-of-year]     [_ _ expr] (h2x/->integer (date-format "D" (h2x/->timestamp expr))))
(defmethod sql.qp/date [:databricks-sql :month]           [_ _ expr] [:trunc (h2x/->timestamp expr) (h2x/literal :MM)])
(defmethod sql.qp/date [:databricks-sql :month-of-year]   [_ _ expr] [:month (h2x/->timestamp expr)])
(defmethod sql.qp/date [:databricks-sql :quarter-of-year] [_ _ expr] [:quarter (h2x/->timestamp expr)])
(defmethod sql.qp/date [:databricks-sql :year]            [_ _ expr] [:trunc (h2x/->timestamp expr) (h2x/literal :year)])

(def ^:private date-extract-units
  "See https://spark.apache.org/docs/3.3.0/api/sql/#extract"
  #{:year :y :years :yr :yrs
    :yearofweek
    :quarter :qtr
    :month :mon :mons :months
    :week :w :weeks
    :day :d :days
    :dayofweek :dow
    :dayofweek_iso :dow_iso
    :doy
    :hour :h :hours :hr :hrs
    :minute :m :min :mins :minutes
    :second :s :sec :seconds :secs})

(defmethod driver/db-start-of-week :databricks-sql
  [_]
  :sunday)

(defn- format-date-extract
  [_fn [unit expr]]
  {:pre [(contains? date-extract-units unit)]}
  (let [[expr-sql & expr-args] (sql/format-expr expr {:nested true})]
    (into [(format "extract(%s FROM %s)" (name unit) expr-sql)]
          expr-args)))

(sql/register-fn! ::date-extract #'format-date-extract)

(defn- format-interval
  [_fn [amount unit]]
  {:pre [(number? amount)
         (#{:year :month :week :day :hour :minute :second :millisecond} unit)]}
  [(format "(interval '%d' %s)" (long amount) (name unit))])

(sql/register-fn! ::interval #'format-interval)

(defmethod sql.qp/date [:databricks-sql :day-of-week]
  [driver _unit expr]
  (sql.qp/adjust-day-of-week driver (-> [::date-extract :dow (h2x/->timestamp expr)]
                                        (h2x/with-database-type-info "integer"))))

(defmethod sql.qp/date [:databricks-sql :week]
  [driver _unit expr]
  (let [week-extract-fn (fn [expr]
                          (-> [:date_sub
                               (h2x/+ (h2x/->timestamp expr)
                                      [::interval 1 :day])
                               [::date-extract :dow (h2x/->timestamp expr)]]
                              (h2x/with-database-type-info "timestamp")))]
    (sql.qp/adjust-start-of-week driver week-extract-fn expr)))


(defmethod sql.qp/date [:databricks-sql :week-of-year-iso]
  [_driver _unit expr]
  [:weekofyear (h2x/->timestamp expr)])

(defmethod sql.qp/date [:databricks-sql :quarter]
  [_driver _unit expr]
  [:add_months
   [:trunc (h2x/->timestamp expr) (h2x/literal :year)]
   (h2x/* (h2x/- [:quarter (h2x/->timestamp expr)]
                 1)
          3)])

(defmethod sql.qp/->honeysql [:databricks-sql :replace]
  [driver [_ arg pattern replacement]]
  [:regexp_replace
   (sql.qp/->honeysql driver arg)
   (sql.qp/->honeysql driver pattern)
   (sql.qp/->honeysql driver replacement)])

(defmethod sql.qp/->honeysql [:databricks-sql :regex-match-first]
  [driver [_ arg pattern]]
  [:regexp_extract (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver pattern) 0])

(defmethod sql.qp/->honeysql [:databricks-sql :median]
  [driver [_ arg]]
  [:percentile (sql.qp/->honeysql driver arg) 0.5])

(defmethod sql.qp/->honeysql [:databricks-sql :percentile]
  [driver [_ arg p]]
  [:percentile (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver p)])

(defmethod sql.qp/add-interval-honeysql-form :databricks-sql
  [driver hsql-form amount unit]
  (if (= unit :quarter)
    (recur driver hsql-form (* amount 3) :month)
    (h2x/+ (h2x/->timestamp hsql-form)
           [::interval amount unit])))

(defmethod sql.qp/datetime-diff [:databricks-sql :year]
  [driver _unit x y]
  [:div (sql.qp/datetime-diff driver :month x y) 12])

(defmethod sql.qp/datetime-diff [:databricks-sql :quarter]
  [driver _unit x y]
  [:div (sql.qp/datetime-diff driver :month x y) 3])

(defmethod sql.qp/datetime-diff [:databricks-sql :month]
  [_driver _unit x y]
  (h2x/->integer [:months_between y x]))

(defmethod sql.qp/datetime-diff [:databricks-sql :week]
  [_driver _unit x y]
  [:div [:datediff y x] 7])

(defmethod sql.qp/datetime-diff [:databricks-sql :day]
  [_driver _unit x y]
  [:datediff y x])

(defmethod sql.qp/datetime-diff [:databricks-sql :hour]
  [driver _unit x y]
  [:div (sql.qp/datetime-diff driver :second x y) 3600])

(defmethod sql.qp/datetime-diff [:databricks-sql :minute]
  [driver _unit x y]
  [:div (sql.qp/datetime-diff driver :second x y) 60])

(defmethod sql.qp/datetime-diff [:databricks-sql :second]
  [_driver _unit x y]
  [:-
   [:unix_timestamp y (if (instance? LocalDate y)
                        (h2x/literal "yyyy-MM-dd")
                        (h2x/literal "yyyy-MM-dd HH:mm:ss"))]
   [:unix_timestamp x (if (instance? LocalDate x)
                        (h2x/literal "yyyy-MM-dd")
                        (h2x/literal "yyyy-MM-dd HH:mm:ss"))]])

(defmethod unprepare/unprepare-value [:databricks-sql String]
  [_ ^String s]
  (case *param-splice-style*
    :friendly (str \' (sql.u/escape-sql s :backslashes) \')
    :paranoid (format "decode(unhex('%s'), 'utf-8')" (codecs/bytes->hex (.getBytes s "UTF-8")))))

(defmethod unprepare/unprepare-value [:databricks-sql LocalDate]
  [driver t]
  (unprepare/unprepare-value driver (t/local-date-time t (t/local-time 0))))

(defmethod unprepare/unprepare-value [:databricks-sql OffsetDateTime]
  [_ t]
  (format "to_utc_timestamp('%s', '%s')" (u.date/format-sql (t/local-date-time t)) (t/zone-offset t)))

(defmethod unprepare/unprepare-value [:databricks-sql ZonedDateTime]
  [_ t]
  (format "to_utc_timestamp('%s', '%s')" (u.date/format-sql (t/local-date-time t)) (t/zone-id t)))

(defmethod sql-jdbc.execute/set-parameter [:databricks-sql LocalDate]
  [driver ps i t]
  (sql-jdbc.execute/set-parameter driver ps i (t/local-date-time t (t/local-time 0))))

(defmethod sql-jdbc.execute/read-column-thunk [:databricks-sql Types/TIME]
  [_ ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [t (.getTimestamp rs i)]
      (t/offset-time (t/local-time t) (t/zone-offset 0)))))

(defmethod sql-jdbc.execute/read-column-thunk [:databricks-sql Types/DATE]
  [_ ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [t (.getDate rs i)]
      (t/zoned-date-time (t/local-date t) (t/local-time 0) (t/zone-id "UTC")))))

(defmethod sql-jdbc.execute/read-column-thunk [:databricks-sql Types/TIMESTAMP]
  [_ ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [t (.getTimestamp rs i)]
      (t/zoned-date-time (t/local-date-time t) (t/zone-id "UTC")))))