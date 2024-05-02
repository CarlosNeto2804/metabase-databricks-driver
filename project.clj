(defproject databricks-driver "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [com.databricks/databricks-jdbc "2.6.36"]]
  :repositories [["opensaml" "https://build.shibboleth.net/nexus/content/repositories/releases/"]]
  :repl-options {:init-ns metabase.driver.databricks-sql}
  :paths ["src" "resources"]
  :aliases
  {"dev" {:extra-deps {io.github.metabase/metabase {:git/url "https://github.com/metabase/metabase.git"
                                                     :tag "v0.46.6.4"
                                                     :sha "7c60aca"}}}
   "user/databricks-sql" {:extra-deps {metabase/databricks-sql {:local/root "/driver"}}
                           :jvm-opts ["-Dmb.dev.additional.driver.manifest.paths=/driver/resources/metabase-plugin.yaml"]}})
