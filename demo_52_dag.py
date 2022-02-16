# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""An example DAG demonstrating simple Apache Airflow operators."""

# [START composer_simple]
from __future__ import print_function

# [START composer_simple_define_dag]
import datetime

from airflow import models
# [END composer_simple_define_dag]
# [START composer_simple_operators]
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.providers.google.cloud.operators import bigquery
# [END composer_simple_operators]


# [START composer_simple_define_dag]
default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2018, 1, 1),
}

bq_dataset_name = 'hkt'
location = 'US'
project_id = 'lufeng-dev'

CLEAN_UP_QUERY = f"""
       bq query \
       --use_legacy_sql=false \
       '
       truncate table hkt.dim_customer_diff;
       truncate table hkt.dim_customer_diff_pii;
       '
       """

ENCRPTION_QUERY = f"""
       bq query \
       --use_legacy_sql=false \
       '
       CALL `lufeng-dev.hkt.proc_encryption_cust`();
       '
       """
MERGE_QUERY = f"""
       bq query \
       --use_legacy_sql=false \
       '
       MERGE hkt.dim_customer_pii T
       USING hkt.dim_customer_diff_pii S
       ON T.cust_num = S.cust_num
       WHEN MATCHED THEN
         UPDATE SET  T.cust_name_pii              = S.cust_name_pii   
                   , T.HKT_STAFF                  = S.HKT_STAFF     
                   , T.BLACKLIST_STATUS           = S.BLACKLIST_STATUS       
                   , T.INDUSTRY_TYPE              = S.INDUSTRY_TYPE      
                   , T.CUST_CAT                   = S.CUST_CAT       
                   , T.CLUB_ID                    = S.CLUB_ID       
                   , T.CLUB_DESC                  = S.CLUB_DESC       
                   , T.IDD_SPEND_LEVEL            = S.IDD_SPEND_LEVEL       
                   , T.CUST_CLS_CD                = S.CUST_CLS_CD      
                   , T.SEGMENT                    = S.SEGMENT       
                   , T.MKT_SEG_DESC               = S.MKT_SEG_DESC       
                   , T.MKT_SEG_TYPE_CD            = S.MKT_SEG_TYPE_CD      
                   , T.MKT_SEG_CD                 = S.MKT_SEG_CD       
                   , T.FIRST_DEFECT_OPERATOR      = S.FIRST_DEFECT_OPERATOR       
                   , T.FIRST_DEFECT_DATE          = S.FIRST_DEFECT_DATE     
                   , T.FIRST_DEFECT_MONTH         = S.FIRST_DEFECT_MONTH       
                   , T.FIRST_DEFECT_WEEK          = S.FIRST_DEFECT_WEEK     
                   , T.IDD_006X_REGISTERED_DATE   = S.IDD_006X_REGISTERED_DATE     
                   , T.ACCOUNT_GROUP_NAME         = S.ACCOUNT_GROUP_NAME       
                   , T.ORG_UNIT_SHORT_NAME        = S.ORG_UNIT_SHORT_NAME       
                   , T.ACCOUNT_MANAGER            = S.ACCOUNT_MANAGER       
                   , T.LAST_UPD_DATE              = S.LAST_UPD_DATE     
                   , T.CUST_CHINESE_NAME          = S.CUST_CHINESE_NAME      
                   , T.CUST_FIRST_NAME_PII        = S.CUST_FIRST_NAME_PII       
                   , T.CUST_LAST_NAME_PII         = S.CUST_LAST_NAME_PII       
                   , T.MAILING_ADDRESS_ID         = S.MAILING_ADDRESS_ID     
                   , T.GENDER                     = S.GENDER     
                   , T.DAY_OF_BIRTH_PII           = S.DAY_OF_BIRTH_PII     
                   , T.AGE_GROUP                  = S.AGE_GROUP       
                   , T.AGE                        = S.AGE     
                   , T.CUSTOMER_TYPE              = S.CUSTOMER_TYPE       
                   , T.CUSTOMER_CATEGORY          = S.CUSTOMER_CATEGORY       
                   , T.CONTACT_PERSON             = S.CONTACT_PERSON       
                   , T.TITLE                      = S.TITLE       
                   , T.CUSTOMER_REMARK            = S.CUSTOMER_REMARK       
                   , T.DIRECT_MKT_FLG             = S.DIRECT_MKT_FLG      
                   , T.DISCLOSURE_TO_ASSO_CO      = S.DISCLOSURE_TO_ASSO_CO       
                   , T.DAY_PHONE_NO               = S.DAY_PHONE_NO       
                   , T.NIGHT_PHONE_NO             = S.NIGHT_PHONE_NO       
                   , T.NETVIGATOR_CUST_IND        = S.NETVIGATOR_CUST_IND       
                   , T.CUST_CREATION_DATE         = S.CUST_CREATION_DATE     
                   , T.ID_DOC_TYPE                = S.ID_DOC_TYPE       
                   , T.ID_DOC_NUM                 = S.ID_DOC_NUM       
                   , T.ENGLISH_COMPANY_NAME       = S.ENGLISH_COMPANY_NAME       
                   , T.CHINESE_COMPANY_NAME       = S.CHINESE_COMPANY_NAME       
                   , T.HKT_STAFF_ID               = S.HKT_STAFF_ID       
                   , T.EMAIL_ADDR_PII             = S.EMAIL_ADDR_PII       
                   , T.FAX1_NO_PII                = S.FAX1_NO_PII       
                   , T.PAGER_NO_PII               = S.PAGER_NO_PII       
                   , T.MOBILE_NO_PII              = S.MOBILE_NO_PII       
                   , T.ID_VERIFY_IND              = S.ID_VERIFY_IND       
                   , T.SP_ID                      = S.SP_ID       
                   , T.LANG_WRITTEN               = S.LANG_WRITTEN       
                   , T.LANG_SPOKEN                = S.LANG_SPOKEN       
                   , T.PREMIER_TYPE               = S.PREMIER_TYPE       
                   , T.SUPREME_IND                = S.SUPREME_IND       
                   , T.BOM_CUST_NUM               = S.BOM_CUST_NUM
       WHEN NOT MATCHED THEN
         INSERT (CUST_KEY, SUPER_CUST_NUM, SUPER_CUST_NAME, CUST_NUM, cust_name_pii, HKT_STAFF, BLACKLIST_STATUS, INDUSTRY_TYPE, CUST_CAT, CLUB_ID, CLUB_DESC, IDD_SPEND_LEVEL, CUST_CLS_CD, SEGMENT, MKT_SEG_DESC, MKT_SEG_TYPE_CD, MKT_SEG_CD, FIRST_DEFECT_OPERATOR, FIRST_DEFECT_DATE, FIRST_DEFECT_MONTH, FIRST_DEFECT_WEEK, IDD_006X_REGISTERED_DATE, ACCOUNT_GROUP_NAME, ORG_UNIT_SHORT_NAME, ACCOUNT_MANAGER, LAST_UPD_DATE, CUST_CHINESE_NAME, CUST_FIRST_NAME_PII, CUST_LAST_NAME_PII, MAILING_ADDRESS_ID, GENDER,  DAY_OF_BIRTH_PII, AGE_GROUP, AGE, CUSTOMER_TYPE, CUSTOMER_CATEGORY, CONTACT_PERSON, TITLE, CUSTOMER_REMARK, DIRECT_MKT_FLG, DISCLOSURE_TO_ASSO_CO, DAY_PHONE_NO, NIGHT_PHONE_NO, NETVIGATOR_CUST_IND, CUST_CREATION_DATE, ID_DOC_TYPE, ID_DOC_NUM, ENGLISH_COMPANY_NAME, CHINESE_COMPANY_NAME, HKT_STAFF_ID, EMAIL_ADDR_PII, FAX1_NO_PII, PAGER_NO_PII, MOBILE_NO_PII, ID_VERIFY_IND, SP_ID, LANG_WRITTEN, LANG_SPOKEN, PREMIER_TYPE, SUPREME_IND, BOM_CUST_NUM) 
         VALUES(CUST_KEY, SUPER_CUST_NUM, SUPER_CUST_NAME, CUST_NUM, cust_name_pii, HKT_STAFF, BLACKLIST_STATUS, INDUSTRY_TYPE, CUST_CAT, CLUB_ID, CLUB_DESC, IDD_SPEND_LEVEL, CUST_CLS_CD, SEGMENT, MKT_SEG_DESC, MKT_SEG_TYPE_CD, MKT_SEG_CD, FIRST_DEFECT_OPERATOR, FIRST_DEFECT_DATE, FIRST_DEFECT_MONTH, FIRST_DEFECT_WEEK, IDD_006X_REGISTERED_DATE, ACCOUNT_GROUP_NAME, ORG_UNIT_SHORT_NAME, ACCOUNT_MANAGER, LAST_UPD_DATE, CUST_CHINESE_NAME, CUST_FIRST_NAME_PII, CUST_LAST_NAME_PII, MAILING_ADDRESS_ID, GENDER,  DAY_OF_BIRTH_PII, AGE_GROUP, AGE, CUSTOMER_TYPE, CUSTOMER_CATEGORY, CONTACT_PERSON, TITLE, CUSTOMER_REMARK, DIRECT_MKT_FLG, DISCLOSURE_TO_ASSO_CO, DAY_PHONE_NO, NIGHT_PHONE_NO, NETVIGATOR_CUST_IND, CUST_CREATION_DATE, ID_DOC_TYPE, ID_DOC_NUM, ENGLISH_COMPANY_NAME, CHINESE_COMPANY_NAME, HKT_STAFF_ID, EMAIL_ADDR_PII, FAX1_NO_PII, PAGER_NO_PII, MOBILE_NO_PII, ID_VERIFY_IND, SP_ID, LANG_WRITTEN, LANG_SPOKEN, PREMIER_TYPE, SUPREME_IND, BOM_CUST_NUM)
       '
       """

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
        'hkt_demo_5_2',
        schedule_interval=None,
        default_args=default_dag_args) as dag:
    # [END composer_simple_define_dag]
    # [START composer_simple_operators]
    def greeting():
        import logging
        logging.info('Hello World!')

    # An instance of an operator is called a task. In this case, the
    # hello_python task calls the "greeting" Python function.
   # [START composer_bigquery]
    clean_up_query = bash_operator.BashOperator(
        task_id='clean_up',
        bash_command=CLEAN_UP_QUERY)


    # Likewise, the goodbye_bash task calls a Bash script.
    bq_cmd = "bq load \
             --source_format=CSV \
             --skip_leading_rows=1 \
             --replace=false \
             --field_delimiter='|' \
             lufeng-dev:hkt.dim_customer_diff \
             gs://lufeng-dev/stage/dim_customer_nowtvpoc_pii_diff.datDelimited.txt \
             CUST_KEY:numeric,SUPER_CUST_NUM:string,SUPER_CUST_NAME:string,CUST_NUM:string,CUST_NAME:string,HKT_STAFF:string,BLACKLIST_STATUS:string,INDUSTRY_TYPE:string,CUST_CAT:string,CLUB_ID:string,CLUB_DESC:string,IDD_SPEND_LEVEL:string,CUST_CLS_CD:string,SEGMENT:string,CUST_FIRST_NAME:string,CUST_LAST_NAME:string,MAILING_ADDRESS_ID:numeric,GENDER:string,DAY_OF_BIRTH:timestamp,AGE_GROUP:string,AGE:numeric,CUSTOMER_TYPE:string,CUSTOMER_CATEGORY:string,CONTACT_PERSON:string,TITLE:string,CUSTOMER_REMARK:string,DIRECT_MKT_FLG:string,DISCLOSURE_TO_ASSO_CO:string,DAY_PHONE_NO:string,NIGHT_PHONE_NO:string,NETVIGATOR_CUST_IND:string,CUST_CREATION_DATE:timestamp,ID_DOC_TYPE:string,ID_DOC_NUM:string,ENGLISH_COMPANY_NAME:string,CHINESE_COMPANY_NAME:string,HKT_STAFF_ID:string,EMAIL_ADDR:string,FAX1_NO:string,PAGER_NO:string,MOBILE_NO:string,ID_VERIFY_IND:string,SP_ID:string,LANG_WRITTEN:string,LANG_SPOKEN:string,PREMIER_TYPE:string,SUPREME_IND:string,BOM_CUST_NUM:string"
    load_bash = bash_operator.BashOperator(
        task_id='dim_customer_diff_load',
        bash_command=bq_cmd)

    encrption_query = bash_operator.BashOperator(
        task_id='encrption_query',
        bash_command=ENCRPTION_QUERY)

    merge_query = bash_operator.BashOperator(
        task_id='merge_query',
        bash_command=MERGE_QUERY)  
    # [END composer_simple_operators]

    # [START composer_simple_relationships]
    # Define the order in which the tasks complete by using the >> and <<
    # operators. In this example, hello_python executes before goodbye_bash.
    clean_up_query >> load_bash >> encrption_query >> merge_query
    # [END composer_simple_relationships]
# [END composer_simple]