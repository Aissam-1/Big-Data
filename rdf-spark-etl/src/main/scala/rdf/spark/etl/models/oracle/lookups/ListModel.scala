package rdf.spark.etl.models.oracle.lookups

import rdf.spark.etl.framework.lib.{Conf, Session}
import com.typesafe.config.Config
import rdf.spark.etl.framework.model.oracle.{Oracle, THModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ListModel(implicit conf: Config, spark: SparkSession) extends Oracle(Conf.th) with THModel{
  
    def requestListCategory(listCode: String): DataFrame = {
        super.request("(SELECT DISTINCT LKP_CATG_ID, LKP_SUBCATG_ID FROM RWE_COMMON.TH_LIST_CATEGORY WHERE LIST_CATEGORY_CD ='" + listCode + "') TH_LIST_CATEGORY")
    }
    
    def requestSuppList(cat: Any, subcat: Any): DataFrame = {
        super.request("(SELECT DISTINCT SUPP_LIST_ID, SRC_LIST_CD FROM RWE_COMMON.TH_SUPP_LIST WHERE (LKP_CATG_ID="+cat+" AND LKP_SUBCATG_ID="+subcat+" AND CTRY_CD='"+Session.instance.getCountryCode+"' AND DATA_PROVIDER_CD='"+Session.instance.getProviderName+"')) TH_SUPP_LIST")
    }
  
}