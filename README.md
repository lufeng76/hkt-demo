# 1. Migrate table dim_customer_nowtvpoc
## Step 1 - run the data fusion load job 'load_dim_customer_raw'
NOTES: the job will take 15 min complete, so do not run the job. Just show the customer that the design UI. You can also import the job from the file <b>load_dim_customer_raw-cdap-data-pipeline.json</b>

```
https://sa-datafusion-lufengsh-dev-lufeng-dev-dot-usw2.datafusion.googleusercontent.com/pipelines/ns/default/view/load_dim_customer_raw
```

## Step 2 - encrypt all PII field using Envelope Encrption
### Generate the data encrption key, and then encrpt the DEK, and store the EDEK to KMS
```
# AEAD key
gcloud kms keyrings create "bq-keyring" \
    --location "us"

gcloud kms keys create "bq-key" \
    --location "us" \
    --keyring "bq-keyring" \
    --purpose "encryption"

gcloud kms keys list \
    --location "us" \
    --keyring "bq-keyring"
NAME: projects/lufeng-dev/locations/us/keyRings/bq-keyring/cryptoKeys/bq-key

gcloud projects add-iam-policy-binding lufeng-dev --member=user:lufengsh@google.com --role=roles/cloudkms.cryptoKeyDecrypterViaDelegation

bq --project_id=lufeng-dev query --use_legacy_sql=false "SELECT KEYS.NEW_KEYSET('AEAD_AES_GCM_256') AS raw_keyset"
CI/TjvYOEmQKWAowdHlwZS5nb29nbGVhcGlzLmNvbS9nb29nbGUuY3J5cHRvLnRpbmsuQWVzR2NtS2V5EiIaIPpC+eUySmYa7BEK+1KweJ1HJ1eS4BIIMT2lD5hJM3FfGAEQARiP0472DiAB

echo "CI/TjvYOEmQKWAowdHlwZS5nb29nbGVhcGlzLmNvbS9nb29nbGUuY3J5cHRvLnRpbmsuQWVzR2NtS2V5EiIaIPpC+eUySmYa7BEK+1KweJ1HJ1eS4BIIMT2lD5hJM3FfGAEQARiP0472DiAB" | base64 --decode > /tmp/decoded_key

gcloud kms encrypt --plaintext-file=/tmp/decoded_key --key=projects/lufeng-dev/locations/us/keyRings/bq-keyring/cryptoKeys/bq-key --ciphertext-file=/tmp/bankaccounts_wrapped
od -An --format=o1 /tmp/bankaccounts_wrapped |tr -d '\n'|tr ' ' '\'
```

### Create the table with the encrpted columns
```
create table hkt.dim_customer_pii like hkt.dim_customer_raw;

ALTER TABLE hkt.dim_customer_pii
ADD COLUMN cust_namez_pii BYTES;
ALTER TABLE hkt.dim_customer_pii
ADD COLUMN CUST_FIRST_NAME_PII BYTES;
ALTER TABLE hkt.dim_customer_pii
ADD COLUMN CUST_LAST_NAME_PII BYTES;
ALTER TABLE hkt.dim_customer_pii
ADD COLUMN DAY_OF_BIRTH_PII BYTES;
ALTER TABLE hkt.dim_customer_pii
ADD COLUMN EMAIL_ADDR_PII BYTES;
ALTER TABLE hkt.dim_customer_pii
ADD COLUMN FAX1_NO_PII BYTES;
ALTER TABLE hkt.dim_customer_pii
ADD COLUMN PAGER_NO_PII BYTES;
ALTER TABLE hkt.dim_customer_pii
ADD COLUMN MOBILE_NO_PII BYTES;
```

### Encrpt the data
```
DECLARE KMS_RESOURCE_NAME STRING;
DECLARE FIRST_LEVEL_KEYSET BYTES;
SET KMS_RESOURCE_NAME = "gcp-kms://projects/lufeng-dev/locations/us/keyRings/bq-keyring/cryptoKeys/bq-key";
SET FIRST_LEVEL_KEYSET = b"\012\044\000\177\306\347\213\115\346\016\137\212\221\373\034\015\230\366\232\125\063\302\075\227\075\231\210\064\161\053\315\361\224\010\357\170\151\176\022\225\001\000\340\157\363\044\305\155\252\274\326\044\065\071\364\036\153\014\316\104\365\043\335\123\337\261\325\130\271\172\152\354\343\366\124\263\126\151\301\023\200\346\277\162\337\050\366\340\165\376\373\101\050\053\164\153\104\244\106\244\304\343\254\032\047\043\361\355\340\147\255\135\377\255\064\000\165\237\361\001\245\052\000\001\007\245\065\016\235\302\270\255\340\140\124\316\166\305\334\025\074\110\260\230\260\241\034\053\162\055\254\140\227\133\302\260\130\116\141\041\031\276\276\056\253\001\301\323\232\031\231\366\160\107\204\003\134\367\221\216\221\145\316\141\114\102\157\131\231\275";
insert into hkt.dim_customer_pii (
    CUST_KEY  , 
    SUPER_CUST_NUM  , 
    SUPER_CUST_NAME    , 
    CUST_NUM    , 
    cust_name_pii, 
    HKT_STAFF  , 
    BLACKLIST_STATUS    , 
    INDUSTRY_TYPE   , 
    CUST_CAT    , 
    CLUB_ID    , 
    CLUB_DESC    , 
    IDD_SPEND_LEVEL    , 
    CUST_CLS_CD   , 
    SEGMENT    , 
    MKT_SEG_DESC    , 
    MKT_SEG_TYPE_CD   , 
    MKT_SEG_CD    , 
    FIRST_DEFECT_OPERATOR    , 
    FIRST_DEFECT_DATE  , 
    FIRST_DEFECT_MONTH    , 
    FIRST_DEFECT_WEEK  , 
    IDD_006X_REGISTERED_DATE  , 
    ACCOUNT_GROUP_NAME    , 
    ORG_UNIT_SHORT_NAME    , 
    ACCOUNT_MANAGER    , 
    LAST_UPD_DATE  , 
    CUST_CHINESE_NAME   , 
    CUST_FIRST_NAME_PII    , 
    CUST_LAST_NAME_PII    , 
    MAILING_ADDRESS_ID  , 
    GENDER  , 
    DAY_OF_BIRTH_PII  , 
    AGE_GROUP    , 
    AGE  , 
    CUSTOMER_TYPE    , 
    CUSTOMER_CATEGORY    , 
    CONTACT_PERSON    , 
    TITLE    , 
    CUSTOMER_REMARK    , 
    DIRECT_MKT_FLG   , 
    DISCLOSURE_TO_ASSO_CO    , 
    DAY_PHONE_NO    , 
    NIGHT_PHONE_NO    , 
    NETVIGATOR_CUST_IND    , 
    CUST_CREATION_DATE  , 
    ID_DOC_TYPE    , 
    ID_DOC_NUM    , 
    ENGLISH_COMPANY_NAME    , 
    CHINESE_COMPANY_NAME    , 
    HKT_STAFF_ID    , 
    EMAIL_ADDR_PII    , 
    FAX1_NO_PII    , 
    PAGER_NO_PII    , 
    MOBILE_NO_PII    , 
    ID_VERIFY_IND    , 
    SP_ID    , 
    LANG_WRITTEN    , 
    LANG_SPOKEN    , 
    PREMIER_TYPE    , 
    SUPREME_IND    , 
    BOM_CUST_NUM)
select 
    CUST_KEY  , 
    SUPER_CUST_NUM  , 
    SUPER_CUST_NAME    , 
    CUST_NUM    , 
    AEAD.ENCRYPT(KEYS.KEYSET_CHAIN(KMS_RESOURCE_NAME, FIRST_LEVEL_KEYSET), CUST_NAME, "pii"), 
    HKT_STAFF  , 
    BLACKLIST_STATUS    , 
    INDUSTRY_TYPE   , 
    CUST_CAT    , 
    CLUB_ID    , 
    CLUB_DESC    , 
    IDD_SPEND_LEVEL    , 
    CUST_CLS_CD   , 
    SEGMENT    , 
    MKT_SEG_DESC    , 
    MKT_SEG_TYPE_CD   , 
    MKT_SEG_CD    , 
    FIRST_DEFECT_OPERATOR    , 
    FIRST_DEFECT_DATE  , 
    FIRST_DEFECT_MONTH    , 
    FIRST_DEFECT_WEEK  , 
    IDD_006X_REGISTERED_DATE  , 
    ACCOUNT_GROUP_NAME    , 
    ORG_UNIT_SHORT_NAME    , 
    ACCOUNT_MANAGER    , 
    LAST_UPD_DATE  , 
    CUST_CHINESE_NAME   , 
    AEAD.ENCRYPT(KEYS.KEYSET_CHAIN(KMS_RESOURCE_NAME, FIRST_LEVEL_KEYSET), CUST_FIRST_NAME, "pii"), 
    AEAD.ENCRYPT(KEYS.KEYSET_CHAIN(KMS_RESOURCE_NAME, FIRST_LEVEL_KEYSET), CUST_LAST_NAME, "pii"), 
    MAILING_ADDRESS_ID  , 
    GENDER  , 
    AEAD.ENCRYPT(KEYS.KEYSET_CHAIN(KMS_RESOURCE_NAME, FIRST_LEVEL_KEYSET), cast(DAY_OF_BIRTH as string format 'YYYY-MM-DD'), "pii"), 
    AGE_GROUP    , 
    AGE  , 
    CUSTOMER_TYPE    , 
    CUSTOMER_CATEGORY    , 
    CONTACT_PERSON    , 
    TITLE    , 
    CUSTOMER_REMARK    , 
    DIRECT_MKT_FLG   , 
    DISCLOSURE_TO_ASSO_CO    , 
    DAY_PHONE_NO    , 
    NIGHT_PHONE_NO    , 
    NETVIGATOR_CUST_IND    , 
    CUST_CREATION_DATE  , 
    ID_DOC_TYPE    , 
    ID_DOC_NUM    , 
    ENGLISH_COMPANY_NAME    , 
    CHINESE_COMPANY_NAME    , 
    HKT_STAFF_ID    , 
    AEAD.ENCRYPT(KEYS.KEYSET_CHAIN(KMS_RESOURCE_NAME, FIRST_LEVEL_KEYSET), EMAIL_ADDR, "pii"), 
    AEAD.ENCRYPT(KEYS.KEYSET_CHAIN(KMS_RESOURCE_NAME, FIRST_LEVEL_KEYSET), FAX1_NO, "pii"), 
    AEAD.ENCRYPT(KEYS.KEYSET_CHAIN(KMS_RESOURCE_NAME, FIRST_LEVEL_KEYSET), PAGER_NO, "pii"),   
    AEAD.ENCRYPT(KEYS.KEYSET_CHAIN(KMS_RESOURCE_NAME, FIRST_LEVEL_KEYSET), MOBILE_NO, "pii"),   
    ID_VERIFY_IND    , 
    SP_ID    , 
    LANG_WRITTEN    , 
    LANG_SPOKEN    , 
    PREMIER_TYPE    , 
    SUPREME_IND    , 
    BOM_CUST_NUM
from hkt.dim_customer_raw;
```
## Using Data Fusion + DLP for the PII data tokenization
Instead of using AEAD in BQ, another approach for the pii data tokenization is to use Data Fusion + DLP plugin. You can import the job using the file <b>load_dim_customer_dlp-cdap-data-pipeline.json</b>
```
https://sa-datafusion-lufengsh-dev-lufeng-dev-dot-usw2.datafusion.googleusercontent.com/pipelines/ns/default/view/load_dim_customer_dlp
```

# 2. Migrate the table schema dim_vi_custbase
Run the data fusion pipeline 'load_dim_vi_custbase'. You can import the job using the file <b>load_dim_vi_custbase-cdap-data-pipeline.json</b>
```
https://sa-datafusion-lufengsh-dev-lufeng-dev-dot-usw2.datafusion.googleusercontent.com/pipelines/ns/default/view/load_dim_vi_custbase
```
For some reason, the partition info can not be created by this CDF pipeline. Need to manually create a table wiht partition
```
CREATE TABLE
  hkt.dim_vi_custbase_p 
PARTITION BY
  TIMESTAMP_TRUNC(BATCH_DATE, DAY)
AS SELECT * FROM `lufeng-dev.hkt.dim_vi_custbase`;
```

# 3. Load the incremental data file
## Step 1 - ingest the data file
```
# create the diff table
create table hkt.dim_customer_diff like hkt.dim_customer_raw;

# convert the fix length to the csv format
python3 convert.py -i dim_customer_nowtvpoc_pii_diff.dat -c config.txt

# load the data
bq load \
--source_format=CSV \
--skip_leading_rows=1 \
--replace=false \
--field_delimiter='|' \
lufeng-dev:hkt.dim_customer_diff \
gs://lufeng-dev/stage/dim_customer_nowtvpoc_pii_diff.datDelimited.txt \
CUST_KEY:numeric,SUPER_CUST_NUM:string,SUPER_CUST_NAME:string,CUST_NUM:string,CUST_NAME:string,HKT_STAFF:string,BLACKLIST_STATUS:string,INDUSTRY_TYPE:string,CUST_CAT:string,CLUB_ID:string,CLUB_DESC:string,IDD_SPEND_LEVEL:string,CUST_CLS_CD:string,SEGMENT:string,CUST_FIRST_NAME:string,CUST_LAST_NAME:string,MAILING_ADDRESS_ID:numeric,GENDER:string,DAY_OF_BIRTH:timestamp,AGE_GROUP:string,AGE:numeric,CUSTOMER_TYPE:string,CUSTOMER_CATEGORY:string,CONTACT_PERSON:string,TITLE:string,CUSTOMER_REMARK:string,DIRECT_MKT_FLG:string,DISCLOSURE_TO_ASSO_CO:string,DAY_PHONE_NO:string,NIGHT_PHONE_NO:string,NETVIGATOR_CUST_IND:string,CUST_CREATION_DATE:timestamp,ID_DOC_TYPE:string,ID_DOC_NUM:string,ENGLISH_COMPANY_NAME:string,CHINESE_COMPANY_NAME:string,HKT_STAFF_ID:string,EMAIL_ADDR:string,FAX1_NO:string,PAGER_NO:string,MOBILE_NO:string,ID_VERIFY_IND:string,SP_ID:string,LANG_WRITTEN:string,LANG_SPOKEN:string,PREMIER_TYPE:string,SUPREME_IND:string,BOM_CUST_NUM:string                  
```

## Step 2 - Encrpt the PII column
```
create table hkt.dim_customer_diff_pii like hkt.dim_customer_pii;

CREATE OR REPLACE PROCEDURE hkt.proc_encryption_cust() BEGIN
       DECLARE KMS_RESOURCE_NAME STRING;
       DECLARE FIRST_LEVEL_KEYSET BYTES;
       SET KMS_RESOURCE_NAME = "gcp-kms://projects/lufeng-dev/locations/us/keyRings/bq-keyring/cryptoKeys/bq-key";
       SET FIRST_LEVEL_KEYSET = b"\012\044\000\177\306\347\213\115\346\016\137\212\221\373\034\015\230\366\232\125\063\302\075\227\075\231\210\064\161\053\315\361\224\010\357\170\151\176\022\225\001\000\340\157\363\044\305\155\252\274\326\044\065\071\364\036\153\014\316\104\365\043\335\123\337\261\325\130\271\172\152\354\343\366\124\263\126\151\301\023\200\346\277\162\337\050\366\340\165\376\373\101\050\053\164\153\104\244\106\244\304\343\254\032\047\043\361\355\340\147\255\135\377\255\064\000\165\237\361\001\245\052\000\001\007\245\065\016\235\302\270\255\340\140\124\316\166\305\334\025\074\110\260\230\260\241\034\053\162\055\254\140\227\133\302\260\130\116\141\041\031\276\276\056\253\001\301\323\232\031\231\366\160\107\204\003\134\367\221\216\221\145\316\141\114\102\157\131\231\275";
       
       insert into hkt.dim_customer_diff_pii (
           CUST_KEY  , 
           SUPER_CUST_NUM  , 
           SUPER_CUST_NAME    , 
           CUST_NUM    , 
           cust_name_pii, 
           HKT_STAFF  , 
           BLACKLIST_STATUS    , 
           INDUSTRY_TYPE   , 
           CUST_CAT    , 
           CLUB_ID    , 
           CLUB_DESC    , 
           IDD_SPEND_LEVEL    , 
           CUST_CLS_CD   , 
           SEGMENT    , 
           MKT_SEG_DESC    , 
           MKT_SEG_TYPE_CD   , 
           MKT_SEG_CD    , 
           FIRST_DEFECT_OPERATOR    , 
           FIRST_DEFECT_DATE  , 
           FIRST_DEFECT_MONTH    , 
           FIRST_DEFECT_WEEK  , 
           IDD_006X_REGISTERED_DATE  , 
           ACCOUNT_GROUP_NAME    , 
           ORG_UNIT_SHORT_NAME    , 
           ACCOUNT_MANAGER    , 
           LAST_UPD_DATE  , 
           CUST_CHINESE_NAME   , 
           CUST_FIRST_NAME_PII    , 
           CUST_LAST_NAME_PII    , 
           MAILING_ADDRESS_ID  , 
           GENDER  , 
           DAY_OF_BIRTH_PII  , 
           AGE_GROUP    , 
           AGE  , 
           CUSTOMER_TYPE    , 
           CUSTOMER_CATEGORY    , 
           CONTACT_PERSON    , 
           TITLE    , 
           CUSTOMER_REMARK    , 
           DIRECT_MKT_FLG   , 
           DISCLOSURE_TO_ASSO_CO    , 
           DAY_PHONE_NO    , 
           NIGHT_PHONE_NO    , 
           NETVIGATOR_CUST_IND    , 
           CUST_CREATION_DATE  , 
           ID_DOC_TYPE    , 
           ID_DOC_NUM    , 
           ENGLISH_COMPANY_NAME    , 
           CHINESE_COMPANY_NAME    , 
           HKT_STAFF_ID    , 
           EMAIL_ADDR_PII    , 
           FAX1_NO_PII    , 
           PAGER_NO_PII    , 
           MOBILE_NO_PII    , 
           ID_VERIFY_IND    , 
           SP_ID    , 
           LANG_WRITTEN    , 
           LANG_SPOKEN    , 
           PREMIER_TYPE    , 
           SUPREME_IND    , 
           BOM_CUST_NUM)
       select 
           CUST_KEY  , 
           SUPER_CUST_NUM  , 
           SUPER_CUST_NAME    , 
           CUST_NUM    , 
           AEAD.ENCRYPT(KEYS.KEYSET_CHAIN(KMS_RESOURCE_NAME, FIRST_LEVEL_KEYSET), CUST_NAME, "pii"), 
           HKT_STAFF  , 
           BLACKLIST_STATUS    , 
           INDUSTRY_TYPE   , 
           CUST_CAT    , 
           CLUB_ID    , 
           CLUB_DESC    , 
           IDD_SPEND_LEVEL    , 
           CUST_CLS_CD   , 
           SEGMENT    , 
           MKT_SEG_DESC    , 
           MKT_SEG_TYPE_CD   , 
           MKT_SEG_CD    , 
           FIRST_DEFECT_OPERATOR    , 
           FIRST_DEFECT_DATE  , 
           FIRST_DEFECT_MONTH    , 
           FIRST_DEFECT_WEEK  , 
           IDD_006X_REGISTERED_DATE  , 
           ACCOUNT_GROUP_NAME    , 
           ORG_UNIT_SHORT_NAME    , 
           ACCOUNT_MANAGER    , 
           LAST_UPD_DATE  , 
           CUST_CHINESE_NAME   , 
           AEAD.ENCRYPT(KEYS.KEYSET_CHAIN(KMS_RESOURCE_NAME, FIRST_LEVEL_KEYSET), CUST_FIRST_NAME, "pii"), 
           AEAD.ENCRYPT(KEYS.KEYSET_CHAIN(KMS_RESOURCE_NAME, FIRST_LEVEL_KEYSET), CUST_LAST_NAME, "pii"), 
           MAILING_ADDRESS_ID  , 
           GENDER  , 
           AEAD.ENCRYPT(KEYS.KEYSET_CHAIN(KMS_RESOURCE_NAME, FIRST_LEVEL_KEYSET), cast(DAY_OF_BIRTH as string format 'YYYY-MM-DD'), "pii"), 
           AGE_GROUP    , 
           AGE  , 
           CUSTOMER_TYPE    , 
           CUSTOMER_CATEGORY    , 
           CONTACT_PERSON    , 
           TITLE    , 
           CUSTOMER_REMARK    , 
           DIRECT_MKT_FLG   , 
           DISCLOSURE_TO_ASSO_CO    , 
           DAY_PHONE_NO    , 
           NIGHT_PHONE_NO    , 
           NETVIGATOR_CUST_IND    , 
           CUST_CREATION_DATE  , 
           ID_DOC_TYPE    , 
           ID_DOC_NUM    , 
           ENGLISH_COMPANY_NAME    , 
           CHINESE_COMPANY_NAME    , 
           HKT_STAFF_ID    , 
           AEAD.ENCRYPT(KEYS.KEYSET_CHAIN(KMS_RESOURCE_NAME, FIRST_LEVEL_KEYSET), EMAIL_ADDR, "pii"), 
           AEAD.ENCRYPT(KEYS.KEYSET_CHAIN(KMS_RESOURCE_NAME, FIRST_LEVEL_KEYSET), FAX1_NO, "pii"), 
           AEAD.ENCRYPT(KEYS.KEYSET_CHAIN(KMS_RESOURCE_NAME, FIRST_LEVEL_KEYSET), PAGER_NO, "pii"),   
           AEAD.ENCRYPT(KEYS.KEYSET_CHAIN(KMS_RESOURCE_NAME, FIRST_LEVEL_KEYSET), MOBILE_NO, "pii"),   
           ID_VERIFY_IND    , 
           SP_ID    , 
           LANG_WRITTEN    , 
           LANG_SPOKEN    , 
           PREMIER_TYPE    , 
           SUPREME_IND    , 
           BOM_CUST_NUM
       from hkt.dim_customer_diff;
END;  

CALL `lufeng-dev.hkt.proc_encryption_cust`();
```

## Step 3 - merge the diff table to the base table
```
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
  VALUES(CUST_KEY, SUPER_CUST_NUM, SUPER_CUST_NAME, CUST_NUM, cust_name_pii, HKT_STAFF, BLACKLIST_STATUS, INDUSTRY_TYPE, CUST_CAT, CLUB_ID, CLUB_DESC, IDD_SPEND_LEVEL, CUST_CLS_CD, SEGMENT, MKT_SEG_DESC, MKT_SEG_TYPE_CD, MKT_SEG_CD, FIRST_DEFECT_OPERATOR, FIRST_DEFECT_DATE, FIRST_DEFECT_MONTH, FIRST_DEFECT_WEEK, IDD_006X_REGISTERED_DATE, ACCOUNT_GROUP_NAME, ORG_UNIT_SHORT_NAME, ACCOUNT_MANAGER, LAST_UPD_DATE, CUST_CHINESE_NAME, CUST_FIRST_NAME_PII, CUST_LAST_NAME_PII, MAILING_ADDRESS_ID, GENDER,  DAY_OF_BIRTH_PII, AGE_GROUP, AGE, CUSTOMER_TYPE, CUSTOMER_CATEGORY, CONTACT_PERSON, TITLE, CUSTOMER_REMARK, DIRECT_MKT_FLG, DISCLOSURE_TO_ASSO_CO, DAY_PHONE_NO, NIGHT_PHONE_NO, NETVIGATOR_CUST_IND, CUST_CREATION_DATE, ID_DOC_TYPE, ID_DOC_NUM, ENGLISH_COMPANY_NAME, CHINESE_COMPANY_NAME, HKT_STAFF_ID, EMAIL_ADDR_PII, FAX1_NO_PII, PAGER_NO_PII, MOBILE_NO_PII, ID_VERIFY_IND, SP_ID, LANG_WRITTEN, LANG_SPOKEN, PREMIER_TYPE, SUPREME_IND, BOM_CUST_NUM);
```

## Step 4 - create a view with encrpted PII columns
```
CREATE VIEW hkt.dim_customer_encrypt_v
AS SELECT 
           CUST_KEY  , 
           SUPER_CUST_NUM  , 
           SUPER_CUST_NAME    , 
           CUST_NUM    , 
           cust_name_pii as cust_name, 
           HKT_STAFF  , 
           BLACKLIST_STATUS    , 
           INDUSTRY_TYPE   , 
           CUST_CAT    , 
           CLUB_ID    , 
           CLUB_DESC    , 
           IDD_SPEND_LEVEL    , 
           CUST_CLS_CD   , 
           SEGMENT    , 
           MKT_SEG_DESC    , 
           MKT_SEG_TYPE_CD   , 
           MKT_SEG_CD    , 
           FIRST_DEFECT_OPERATOR    , 
           FIRST_DEFECT_DATE  , 
           FIRST_DEFECT_MONTH    , 
           FIRST_DEFECT_WEEK  , 
           IDD_006X_REGISTERED_DATE  , 
           ACCOUNT_GROUP_NAME    , 
           ORG_UNIT_SHORT_NAME    , 
           ACCOUNT_MANAGER    , 
           LAST_UPD_DATE  , 
           CUST_CHINESE_NAME   , 
           CUST_FIRST_NAME_PII   as CUST_FIRST_NAME , 
           CUST_LAST_NAME_PII    as CUST_LAST_NAME, 
           MAILING_ADDRESS_ID  , 
           GENDER  , 
           DAY_OF_BIRTH_PII  as DAY_OF_BIRTH, 
           AGE_GROUP    , 
           AGE  , 
           CUSTOMER_TYPE    , 
           CUSTOMER_CATEGORY    , 
           CONTACT_PERSON    , 
           TITLE    , 
           CUSTOMER_REMARK    , 
           DIRECT_MKT_FLG   , 
           DISCLOSURE_TO_ASSO_CO    , 
           DAY_PHONE_NO    , 
           NIGHT_PHONE_NO    , 
           NETVIGATOR_CUST_IND    , 
           CUST_CREATION_DATE  , 
           ID_DOC_TYPE    , 
           ID_DOC_NUM    , 
           ENGLISH_COMPANY_NAME    , 
           CHINESE_COMPANY_NAME    , 
           HKT_STAFF_ID    , 
           EMAIL_ADDR_PII   as EMAIL_ADDR , 
           FAX1_NO_PII   as FAX1_NO , 
           PAGER_NO_PII  as PAGER_NO  , 
           MOBILE_NO_PII  as  MOBILE_NO , 
           ID_VERIFY_IND    , 
           SP_ID    , 
           LANG_WRITTEN    , 
           LANG_SPOKEN    , 
           PREMIER_TYPE    , 
           SUPREME_IND    , 
           BOM_CUST_NUM
FROM hkt.dim_customer_pii
;
```

## Step 5 - create a view with instant decrption of PII columns
```
CREATE VIEW hkt.dim_customer_decrypt_v
AS SELECT 
           CUST_KEY  , 
           SUPER_CUST_NUM  , 
           SUPER_CUST_NAME    , 
           CUST_NUM    , 
           CAST(AEAD.DECRYPT_BYTES(KEYS.KEYSET_CHAIN("gcp-kms://projects/lufeng-dev/locations/us/keyRings/bq-keyring/cryptoKeys/bq-key", b"\012\044\000\177\306\347\213\115\346\016\137\212\221\373\034\015\230\366\232\125\063\302\075\227\075\231\210\064\161\053\315\361\224\010\357\170\151\176\022\225\001\000\340\157\363\044\305\155\252\274\326\044\065\071\364\036\153\014\316\104\365\043\335\123\337\261\325\130\271\172\152\354\343\366\124\263\126\151\301\023\200\346\277\162\337\050\366\340\165\376\373\101\050\053\164\153\104\244\106\244\304\343\254\032\047\043\361\355\340\147\255\135\377\255\064\000\165\237\361\001\245\052\000\001\007\245\065\016\235\302\270\255\340\140\124\316\166\305\334\025\074\110\260\230\260\241\034\053\162\055\254\140\227\133\302\260\130\116\141\041\031\276\276\056\253\001\301\323\232\031\231\366\160\107\204\003\134\367\221\216\221\145\316\141\114\102\157\131\231\275"), cust_name_pii, CAST("pii" AS BYTES)) AS STRING) as cust_name, 
           HKT_STAFF  , 
           BLACKLIST_STATUS    , 
           INDUSTRY_TYPE   , 
           CUST_CAT    , 
           CLUB_ID    , 
           CLUB_DESC    , 
           IDD_SPEND_LEVEL    , 
           CUST_CLS_CD   , 
           SEGMENT    , 
           MKT_SEG_DESC    , 
           MKT_SEG_TYPE_CD   , 
           MKT_SEG_CD    , 
           FIRST_DEFECT_OPERATOR    , 
           FIRST_DEFECT_DATE  , 
           FIRST_DEFECT_MONTH    , 
           FIRST_DEFECT_WEEK  , 
           IDD_006X_REGISTERED_DATE  , 
           ACCOUNT_GROUP_NAME    , 
           ORG_UNIT_SHORT_NAME    , 
           ACCOUNT_MANAGER    , 
           LAST_UPD_DATE  , 
           CUST_CHINESE_NAME   , 
           CAST(AEAD.DECRYPT_BYTES(KEYS.KEYSET_CHAIN("gcp-kms://projects/lufeng-dev/locations/us/keyRings/bq-keyring/cryptoKeys/bq-key", b"\012\044\000\177\306\347\213\115\346\016\137\212\221\373\034\015\230\366\232\125\063\302\075\227\075\231\210\064\161\053\315\361\224\010\357\170\151\176\022\225\001\000\340\157\363\044\305\155\252\274\326\044\065\071\364\036\153\014\316\104\365\043\335\123\337\261\325\130\271\172\152\354\343\366\124\263\126\151\301\023\200\346\277\162\337\050\366\340\165\376\373\101\050\053\164\153\104\244\106\244\304\343\254\032\047\043\361\355\340\147\255\135\377\255\064\000\165\237\361\001\245\052\000\001\007\245\065\016\235\302\270\255\340\140\124\316\166\305\334\025\074\110\260\230\260\241\034\053\162\055\254\140\227\133\302\260\130\116\141\041\031\276\276\056\253\001\301\323\232\031\231\366\160\107\204\003\134\367\221\216\221\145\316\141\114\102\157\131\231\275"), CUST_FIRST_NAME_PII, CAST("pii" AS BYTES)) AS STRING) as CUST_FIRST_NAME, 
           CAST(AEAD.DECRYPT_BYTES(KEYS.KEYSET_CHAIN("gcp-kms://projects/lufeng-dev/locations/us/keyRings/bq-keyring/cryptoKeys/bq-key", b"\012\044\000\177\306\347\213\115\346\016\137\212\221\373\034\015\230\366\232\125\063\302\075\227\075\231\210\064\161\053\315\361\224\010\357\170\151\176\022\225\001\000\340\157\363\044\305\155\252\274\326\044\065\071\364\036\153\014\316\104\365\043\335\123\337\261\325\130\271\172\152\354\343\366\124\263\126\151\301\023\200\346\277\162\337\050\366\340\165\376\373\101\050\053\164\153\104\244\106\244\304\343\254\032\047\043\361\355\340\147\255\135\377\255\064\000\165\237\361\001\245\052\000\001\007\245\065\016\235\302\270\255\340\140\124\316\166\305\334\025\074\110\260\230\260\241\034\053\162\055\254\140\227\133\302\260\130\116\141\041\031\276\276\056\253\001\301\323\232\031\231\366\160\107\204\003\134\367\221\216\221\145\316\141\114\102\157\131\231\275"), CUST_LAST_NAME_PII, CAST("pii" AS BYTES)) AS STRING) as CUST_LAST_NAME, 
           MAILING_ADDRESS_ID  , 
           GENDER  , 
           CAST(AEAD.DECRYPT_BYTES(KEYS.KEYSET_CHAIN("gcp-kms://projects/lufeng-dev/locations/us/keyRings/bq-keyring/cryptoKeys/bq-key", b"\012\044\000\177\306\347\213\115\346\016\137\212\221\373\034\015\230\366\232\125\063\302\075\227\075\231\210\064\161\053\315\361\224\010\357\170\151\176\022\225\001\000\340\157\363\044\305\155\252\274\326\044\065\071\364\036\153\014\316\104\365\043\335\123\337\261\325\130\271\172\152\354\343\366\124\263\126\151\301\023\200\346\277\162\337\050\366\340\165\376\373\101\050\053\164\153\104\244\106\244\304\343\254\032\047\043\361\355\340\147\255\135\377\255\064\000\165\237\361\001\245\052\000\001\007\245\065\016\235\302\270\255\340\140\124\316\166\305\334\025\074\110\260\230\260\241\034\053\162\055\254\140\227\133\302\260\130\116\141\041\031\276\276\056\253\001\301\323\232\031\231\366\160\107\204\003\134\367\221\216\221\145\316\141\114\102\157\131\231\275"), DAY_OF_BIRTH_PII, CAST("pii" AS BYTES)) AS STRING) as DAY_OF_BIRTH, 
           AGE_GROUP    , 
           AGE  , 
           CUSTOMER_TYPE    , 
           CUSTOMER_CATEGORY    , 
           CONTACT_PERSON    , 
           TITLE    , 
           CUSTOMER_REMARK    , 
           DIRECT_MKT_FLG   , 
           DISCLOSURE_TO_ASSO_CO    , 
           DAY_PHONE_NO    , 
           NIGHT_PHONE_NO    , 
           NETVIGATOR_CUST_IND    , 
           CUST_CREATION_DATE  , 
           ID_DOC_TYPE    , 
           ID_DOC_NUM    , 
           ENGLISH_COMPANY_NAME    , 
           CHINESE_COMPANY_NAME    , 
           HKT_STAFF_ID    , 
           CAST(AEAD.DECRYPT_BYTES(KEYS.KEYSET_CHAIN("gcp-kms://projects/lufeng-dev/locations/us/keyRings/bq-keyring/cryptoKeys/bq-key", b"\012\044\000\177\306\347\213\115\346\016\137\212\221\373\034\015\230\366\232\125\063\302\075\227\075\231\210\064\161\053\315\361\224\010\357\170\151\176\022\225\001\000\340\157\363\044\305\155\252\274\326\044\065\071\364\036\153\014\316\104\365\043\335\123\337\261\325\130\271\172\152\354\343\366\124\263\126\151\301\023\200\346\277\162\337\050\366\340\165\376\373\101\050\053\164\153\104\244\106\244\304\343\254\032\047\043\361\355\340\147\255\135\377\255\064\000\165\237\361\001\245\052\000\001\007\245\065\016\235\302\270\255\340\140\124\316\166\305\334\025\074\110\260\230\260\241\034\053\162\055\254\140\227\133\302\260\130\116\141\041\031\276\276\056\253\001\301\323\232\031\231\366\160\107\204\003\134\367\221\216\221\145\316\141\114\102\157\131\231\275"), EMAIL_ADDR_PII, CAST("pii" AS BYTES)) AS STRING) as EMAIL_ADDR, 
           CAST(AEAD.DECRYPT_BYTES(KEYS.KEYSET_CHAIN("gcp-kms://projects/lufeng-dev/locations/us/keyRings/bq-keyring/cryptoKeys/bq-key", b"\012\044\000\177\306\347\213\115\346\016\137\212\221\373\034\015\230\366\232\125\063\302\075\227\075\231\210\064\161\053\315\361\224\010\357\170\151\176\022\225\001\000\340\157\363\044\305\155\252\274\326\044\065\071\364\036\153\014\316\104\365\043\335\123\337\261\325\130\271\172\152\354\343\366\124\263\126\151\301\023\200\346\277\162\337\050\366\340\165\376\373\101\050\053\164\153\104\244\106\244\304\343\254\032\047\043\361\355\340\147\255\135\377\255\064\000\165\237\361\001\245\052\000\001\007\245\065\016\235\302\270\255\340\140\124\316\166\305\334\025\074\110\260\230\260\241\034\053\162\055\254\140\227\133\302\260\130\116\141\041\031\276\276\056\253\001\301\323\232\031\231\366\160\107\204\003\134\367\221\216\221\145\316\141\114\102\157\131\231\275"), FAX1_NO_PII, CAST("pii" AS BYTES)) AS STRING) as FAX1_NO, 
           CAST(AEAD.DECRYPT_BYTES(KEYS.KEYSET_CHAIN("gcp-kms://projects/lufeng-dev/locations/us/keyRings/bq-keyring/cryptoKeys/bq-key", b"\012\044\000\177\306\347\213\115\346\016\137\212\221\373\034\015\230\366\232\125\063\302\075\227\075\231\210\064\161\053\315\361\224\010\357\170\151\176\022\225\001\000\340\157\363\044\305\155\252\274\326\044\065\071\364\036\153\014\316\104\365\043\335\123\337\261\325\130\271\172\152\354\343\366\124\263\126\151\301\023\200\346\277\162\337\050\366\340\165\376\373\101\050\053\164\153\104\244\106\244\304\343\254\032\047\043\361\355\340\147\255\135\377\255\064\000\165\237\361\001\245\052\000\001\007\245\065\016\235\302\270\255\340\140\124\316\166\305\334\025\074\110\260\230\260\241\034\053\162\055\254\140\227\133\302\260\130\116\141\041\031\276\276\056\253\001\301\323\232\031\231\366\160\107\204\003\134\367\221\216\221\145\316\141\114\102\157\131\231\275"), PAGER_NO_PII, CAST("pii" AS BYTES)) AS STRING) as PAGER_NO, 
           CAST(AEAD.DECRYPT_BYTES(KEYS.KEYSET_CHAIN("gcp-kms://projects/lufeng-dev/locations/us/keyRings/bq-keyring/cryptoKeys/bq-key", b"\012\044\000\177\306\347\213\115\346\016\137\212\221\373\034\015\230\366\232\125\063\302\075\227\075\231\210\064\161\053\315\361\224\010\357\170\151\176\022\225\001\000\340\157\363\044\305\155\252\274\326\044\065\071\364\036\153\014\316\104\365\043\335\123\337\261\325\130\271\172\152\354\343\366\124\263\126\151\301\023\200\346\277\162\337\050\366\340\165\376\373\101\050\053\164\153\104\244\106\244\304\343\254\032\047\043\361\355\340\147\255\135\377\255\064\000\165\237\361\001\245\052\000\001\007\245\065\016\235\302\270\255\340\140\124\316\166\305\334\025\074\110\260\230\260\241\034\053\162\055\254\140\227\133\302\260\130\116\141\041\031\276\276\056\253\001\301\323\232\031\231\366\160\107\204\003\134\367\221\216\221\145\316\141\114\102\157\131\231\275"), MOBILE_NO_PII, CAST("pii" AS BYTES)) AS STRING) as MOBILE_NO, 
           ID_VERIFY_IND    , 
           SP_ID    , 
           LANG_WRITTEN    , 
           LANG_SPOKEN    , 
           PREMIER_TYPE    , 
           SUPREME_IND    , 
           BOM_CUST_NUM
FROM hkt.dim_customer_pii;
```

## Step 6 - Create a job flow, and call it via remote command
The job flow file <b>demo_52_dag.py</b> can be deployed to the Cloud Composer
```
https://838071c403dd4b80849090abf8161e52-dot-us-west2.composer.googleusercontent.com/home
```

Invoke the flow via remote command
```
# invoke composer job
gcloud composer environments run hkt-demo \
    --location us-west2 dags trigger -- hkt_demo_5_2 \
    --run-id=5077
```

# 3. Data loading performance test

load the data using 'bq load' command line tool
```
python3 convert.py -i dim_customer_nowtvpoc_pii.dat -c config.txt
gsutil cp dim_customer_nowtvpoc_pii.datDelimited.txt gs://lufeng-dev/stage/

create table hkt.dim_customer_raw2 like hkt.dim_customer_raw;

truncate table hkt.dim_customer_raw2;

bq load \
--source_format=CSV \
--skip_leading_rows=1 \
--replace=false \
--max_bad_records=10000 \
--ignore_unknown_values=true \
--allow_quoted_newlines=true \
--field_delimiter='|' \
lufeng-dev:hkt.dim_customer_raw2 \
gs://lufeng-dev/stage/dim_customer_nowtvpoc_pii.datDelimited.txt \
CUST_KEY:numeric,SUPER_CUST_NUM:string,SUPER_CUST_NAME:string,CUST_NUM:string,CUST_NAME:string,HKT_STAFF:string,BLACKLIST_STATUS:string,INDUSTRY_TYPE:string,CUST_CAT:string,CLUB_ID:string,CLUB_DESC:string,IDD_SPEND_LEVEL:string,CUST_CLS_CD:string,SEGMENT:string,CUST_FIRST_NAME:string,CUST_LAST_NAME:string,MAILING_ADDRESS_ID:numeric,GENDER:string,DAY_OF_BIRTH:timestamp,AGE_GROUP:string,AGE:numeric,CUSTOMER_TYPE:string,CUSTOMER_CATEGORY:string,CONTACT_PERSON:string,TITLE:string,CUSTOMER_REMARK:string,DIRECT_MKT_FLG:string,DISCLOSURE_TO_ASSO_CO:string,DAY_PHONE_NO:string,NIGHT_PHONE_NO:string,NETVIGATOR_CUST_IND:string,CUST_CREATION_DATE:timestamp,ID_DOC_TYPE:string,ID_DOC_NUM:string,ENGLISH_COMPANY_NAME:string,CHINESE_COMPANY_NAME:string,HKT_STAFF_ID:string,EMAIL_ADDR:string,FAX1_NO:string,PAGER_NO:string,MOBILE_NO:string,ID_VERIFY_IND:string,SP_ID:string,LANG_WRITTEN:string,LANG_SPOKEN:string,PREMIER_TYPE:string,SUPREME_IND:string,BOM_CUST_NUM:string                  
```

Load the data using the dataflow 
```
python fixlengh_ingestion.py --project=$PROJECT --region=us-west2 \
       --runner=DataflowRunner --staging_location=gs://$PROJECT/test \
       --temp_location gs://$PROJECT/test \
       --num_workers 80 \
       --autoscaling_algorithm NONE \
       --input gs://$PROJECT/stage/dm_vi_custbase_nowtvpoc_pii.dat \
       --output hkt.dim_vi_custbase_p \
       --save_main_session
       

```

Here is the test result with different cores:
* 6min10 (120 and 160)
* 7min35 (40)
* 6min26 (80)
