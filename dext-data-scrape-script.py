!pip3 install pyotp
import pyotp
import requests
import json
import pandas as pd
import numpy as np
import pandas_gbq as gbq
import numpy as np
import timeit
import re  # may or may not require
import os
import shutil
import glob
import logging
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime, timedelta
from time import sleep
import concurrent.futures
import gc
import sys
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from typing import Optional, Union, List, Tuple



requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logFile = "dext_extract_logs.log"
if os.path.exists(logFile):
    os.remove(logFile)

logFormatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

# fileHandler = logging.FileHandler(logFile, 'w', 'utf-8')
# fileHandler.setFormatter(logFormatter)
# logger.addHandler(fileHandler)


class Configs:
    # @title Initialization
    ### Credentials and key.json
    # SG
    RECEIPT_BANK_USER = "erwan@sleek.sg"  # rb
    RECEIPT_BANK_PW = "4hBLd7pQZVzUJTu?"  # rb
    RECEIPT_BANK_SECRET_KEY = "NBHZNEZR4WX2CPZH" # rb

    RECEIPT_BANK_USER2 = "alvin.reyes@sleek.com"  # rb
    RECEIPT_BANK_PW2 = "dextapp2021"  # rb

    HUBDOC_LOGIN = "erwan@sleek.com"  # hd
    HUBDOC_PW = "GUd52MY8wjpPuEE?"  # hd

    LOGIN_ID = "erwan@sleek.com"  # xero
    LOGIN_PW = "bLnYmx5daAbZZ3T"  # xero
    # UK
    RECEIPT_BANK_USER3 = "erwan@sleek.com"  # rb
    RECEIPT_BANK_PW3 = "7JEdR4ryA6228KD?"  # rb

    user_creds_list = [
        ('email1@domain.sg','PASSWORD1',"USER_OTP_PRIVATE_KEY"),
        ('email2@domain.com','PASSWORD1','USER_OTP_PRIVATE_KEY'),
        ('email3@domain.com','PASSWORD1','USER_OTP_PRIVATE_KEY'),
        ('email4@domain.com','PASSWORD1','USER_OTP_PRIVATE_KEY'),
        ('email5@domain.com','PASSWORD1','USER_OTP_PRIVATE_KEY'),
        ("email6@domain.com", "PASSWORD1",'USER_OTP_PRIVATE_KEY'),
        ("email7@domain.com", "PASSWORD1",'USER_OTP_PRIVATE_KEY'),
        ("email8@domain.com", "PASSWORD1",'USER_OTP_PRIVATE_KEY'),
        ("email9@domain.com", "PASSWORD1",'USER_OTP_PRIVATE_KEY'),
        ("email0@domain.com", "PASSWORD1",'USER_OTP_PRIVATE_KEY')
    ]

    HOST = "https://app.dext.com"
    GRAPH_URL = HOST + "/graph/api"
    CLIENT_URL = HOST + "/gamma/client-view"

    NUM_SECOND_SLEEP_TO_RECREATE_SESSION = 10
    MAX_FAILED_REQUESTS = 20
    NUM_THREADING = 10
    NUM_DAYS_PUBLISH_HIST = 3

    CURRENT_SCRIPT_FOLDER = os.getcwd()
    TMP_FOLDER = os.path.join(CURRENT_SCRIPT_FOLDER, "tmp")

    BIGQUERY_DATASET = "hubdoc_dext_data"
    BIGQUERY_PROJECT_ID = "bigquery-production-309309"

    PRACTICE_ID = 'QWNjb3VudC04MjY0NDc='


class ReceiptBankSession(requests.Session):
    def __init__(self, user_email, user_password, user_otp):
        super().__init__()
        self.host = Configs.HOST
        self.login_email = user_email
        self.login_password = user_password
        self.login_otp = user_otp
        self.logger = logging.getLogger()

    def login_receipt_bank(self):
        """Create session with default email and password
        """
        login_url = self.host + "/login"
        payload = {
            "user_login[email]": self.login_email,
            "user_login[password]": self.login_password,
        }
        self.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36"
            }
        )
        self.hooks["response"].append(self.rb_csrf_hook)
        self.verify = True
        self.post(login_url, data=payload)
        login_check = self.post(login_url, data=payload)
        print(login_check.status_code)
        self.logger.info("Receipt Bank login success")
        # OTP

        otp_url = self.host + "/two-factor-authentication"
        totp = pyotp.TOTP(self.login_otp)
        otp_key = totp.now()
        payload_otp = {
            "security_two_factor_authentication_forms_login[code]": otp_key,
            "security_two_factor_authentication_forms_login[keep_signed]": 1
        }
        self.post(otp_url, data=payload_otp)
        otp_check = self.post(login_url, data=payload)
        print(otp_check.status_code)
        self.logger.info("Receipt Bank otp success")


    def rb_csrf_hook(self, response, *args, **kwargs):
        find_csrf = re.findall(
            r'<meta name="csrf-token" content="[\S]*"', response.text
        )
        if len(find_csrf) != 0:
            csrf_token = find_csrf[0].split('"')[-2]
            self.headers.update(
                {
                    "accept": "*/*",
                    "x-csrf-token": csrf_token,
                    "x-requested-with": "XMLHttpRequest",
                }
            )
            self.logger.info(f"Updated rb csrf:{csrf_token}")
        else:
            pass

class BigQueryClass:
    @staticmethod
    def get_google_creds():
        # key.json
        json_string = {
            "type": "service_account",
            "project_id": "PROJECT",
            "private_key_id": "PKEY_ID",
            "private_key": "PKEY",
            "client_email": "SERVICE ACCOUNT EMAIL",
            "client_id": "CLIENT_ID",
            "auth_uri": "AUTH",
            "token_uri": "TOKEN_URI",
            "auth_provider_x509_cert_url": "AUTH_CERT",
            "client_x509_cert_url": "CLIENT_CERT",
        }

        with open("keyjson.json", "w", encoding="utf-8") as f:
            json.dump(json_string, f, ensure_ascii=False, indent=4)

        credentials = service_account.Credentials.from_service_account_file(
            "keyjson.json",
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return credentials

    @staticmethod
    def get_client_bigquery():
        credentials = BigQueryClass.get_google_creds()
        client = bigquery.Client(
            credentials=credentials,
            project=Configs.BIGQUERY_PROJECT_ID,
            location="asia-southeast1",
        )
        return client

    @staticmethod
    def inititalize_bigquery_session():
        credentials = BigQueryClass.get_google_creds()
        gbq.context.credentials = credentials
        gbq.context.project = Configs.BIGQUERY_PROJECT_ID

    @staticmethod
    def get_bigquery_table_name(param_folder_name: str, is_temp: bool = True) -> str:
        if is_temp:
            table_postfix = "_tmp"
        else:
            table_postfix = ""

        if param_folder_name == "archived_costs_publish_info":
            table_name = f"tb_dext_directload_archived_costs_publish_info{table_postfix}"
        else:
            table_name = f"tb_dext_directload_{param_folder_name}{table_postfix}"

        return table_name

    @staticmethod
    def load_data_to_tables(
        source_table: str,
        destination_table: str,
        mode: str
    ):
        logger = logging.getLogger()
        logger.info(f"Load data from `{source_table}` to `{destination_table}` with mode {mode}...")

        if mode == "overwrite":
            sql = f"""
                CREATE OR REPLACE TABLE `{Configs.BIGQUERY_PROJECT_ID}.{Configs.BIGQUERY_DATASET}.{destination_table}`
                AS
                SELECT *
                FROM `{Configs.BIGQUERY_PROJECT_ID}.{Configs.BIGQUERY_DATASET}.{source_table}`;
            """

        elif mode == "append":
            sql = f"""
                CREATE TABLE IF NOT EXISTS `{Configs.BIGQUERY_PROJECT_ID}.{Configs.BIGQUERY_DATASET}.{destination_table}`
                LIKE `{Configs.BIGQUERY_PROJECT_ID}.{Configs.BIGQUERY_DATASET}.{source_table}`;

                INSERT INTO `{Configs.BIGQUERY_PROJECT_ID}.{Configs.BIGQUERY_DATASET}.{destination_table}`
                SELECT
                    company_name,
                    id,
                    action,
                    createdAt,
                    description,
                    accountName,
                    userName,
                    __typename,
                    receipt_id,
                    time_extracted
                FROM `{Configs.BIGQUERY_PROJECT_ID}.{Configs.BIGQUERY_DATASET}.{source_table}`
                WHERE id NOT IN (SELECT DISTINCT id FROM `{Configs.BIGQUERY_PROJECT_ID}.{Configs.BIGQUERY_DATASET}.{destination_table}`);
            """

        logger.info(sql)
        client = BigQueryClass.get_client_bigquery()
        client.query(sql) # Try to get the results of query
        logger.info(f"Load data from `{source_table}` to `{destination_table}` with mode {mode} successfully")


    @staticmethod
    def cleanup_table(table_name: str):
        logger = logging.getLogger()

        table_id = f"{Configs.BIGQUERY_PROJECT_ID}.{Configs.BIGQUERY_DATASET}.{table_name}"

        logger.warning(f"Cleanup table `{table_id}`...")

        client = BigQueryClass.get_client_bigquery()
        client.delete_table(table_id, not_found_ok=True)

        logger.info(f"Drop table `{table_name}` successfully")


    @staticmethod
    def upload_to_bigquery(
        output_folder: str,
        param_folder_name: str,
        df_schema: pd.DataFrame = pd.DataFrame(),
        write_mode: str = "overwrite"
    ) -> Optional[str]:
        logger = logging.getLogger()
        table_name = BigQueryClass.get_bigquery_table_name(param_folder_name, is_temp=True)
        table_name_update = BigQueryClass.get_bigquery_table_name(param_folder_name, is_temp=False)

        logger.info(f"Uploading to bigquery table `{table_name}`...")

        credentials = BigQueryClass.get_google_creds()

        for parquet_file in glob.glob(os.path.join(output_folder, "*.parquet")):
            file_name = os.path.basename(parquet_file)
            data_df = pd.read_parquet(parquet_file)

            logger.info(f"Loaded data from `{file_name}`, total records {len(data_df)}")

            data_df = pd.concat([df_schema, data_df], axis=0, ignore_index=True)
            data_df = data_df.replace({None: np.nan})
            data_df = data_df.replace({np.nan: ""})
            data_df = data_df.astype("str")

            if len(data_df) == 0:
                logger.info(f"File `{file_name}` is empty")
                continue

            if param_folder_name == "cies":
                try:
                    table_id = f"{Configs.BIGQUERY_PROJECT_ID}.{Configs.BIGQUERY_DATASET}.{table_name}"
                    logger.info(f"Uploading to table {table_id}...")

                    client = BigQueryClass.get_client_bigquery()
                    job_config = bigquery.LoadJobConfig(
                        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                        source_format=bigquery.SourceFormat.PARQUET
                    )
                    job = client.load_table_from_dataframe(
                        data_df,
                        table_id,
                        job_config=job_config
                    )
                    job.result()

                    
                    logger.info(f"Uploaded to table {table_id} successfully.")
                except Exception as e:
                    logger.error(e)

            elif param_folder_name == "archived_costs_publish_info":
                schema = [
                    {"name": "createdAt", "type": "TIMESTAMP"},
                    {"name": "time_extracted", "type": "TIMESTAMP"},
                ]

                try:
                    data_df["createdAt"] = pd.to_datetime(data_df["createdAt"], utc=True)
                    data_df["time_extracted"] = pd.to_datetime(data_df["time_extracted"], utc=True)

                    table_id = f"{Configs.BIGQUERY_PROJECT_ID}.{Configs.BIGQUERY_DATASET}.{table_name}"
                    logger.info(f"Uploading to table {table_id}...")

                    client = BigQueryClass.get_client_bigquery()
                    job_config = bigquery.LoadJobConfig(
                        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                        source_format=bigquery.SourceFormat.PARQUET
                    )
                    job = client.load_table_from_dataframe(
                        data_df,
                        table_id,
                        job_config=job_config
                    )
                    job.result()

                    
                    logger.info(f"Uploaded to table {table_id} successfully.")
                except Exception as e:
                    logger.error(e)

            else:
                schema = [
                    {"name": "unarchivedOn", "type": "DATE"},
                    {"name": "archivedOn", "type": "DATE"},
                    {"name": "baseTaxAmount", "type": "FLOAT64"},
                    {"name": "totalAmount", "type": "FLOAT64"},
                    {"name": "baseTotalAmount", "type": "FLOAT64"},
                    {"name": "createdat", "type": "TIMESTAMP"},
                    {"name": "date", "type": "DATE"},
                    {"name": "dueDate", "type": "DATE"},
                    {"name": "editedOn", "type": "DATE"},
                    {"name": "exportedOn", "type": "DATE"},
                    {"name": "netAmount", "type": "FLOAT64"},
                    {"name": "publishedOn2", "type": "DATE"},
                    {"name": "secondaryTaxAmount", "type": "FLOAT64"},
                    {"name": "taxAmount", "type": "FLOAT64"},
                ]
                try:
                    table_id = f"{Configs.BIGQUERY_PROJECT_ID}.{Configs.BIGQUERY_DATASET}.{table_name}"
                    logger.info(f"Uploading to table {table_id}...")

                    client = BigQueryClass.get_client_bigquery()
                    job_config = bigquery.LoadJobConfig(
                        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                        source_format=bigquery.SourceFormat.PARQUET
                    )
                    job = client.load_table_from_dataframe(
                        data_df,
                        table_id,
                        job_config=job_config
                    )
                    job.result()

                    
                    logger.info(f"Uploaded to table {table_id} successfully.")
                except Exception as e:
                    logger.error(e)

            logger.info(f"Upload file `{file_name}` to bigquery table `{table_name}` successfully, total records {len(data_df)}")

        BigQueryClass.load_data_to_tables(source_table=table_name, destination_table=table_name_update, mode=write_mode)

        logger.info(f"HINT: Tempt table isn't deleted for backup. You could check the data in table `{table_name}`")
        # BigQueryClass.cleanup_table(table_name=table_name)

        return f'Table {table_name_update} upload done!'

    @staticmethod
    def upload_logs_to_bigquery(logs_df: pd.DataFrame, table_name: str = "tb_dext_directload_extraction_logs"):
        logger = logging.getLogger()

        table_id = f"{Configs.BIGQUERY_DATASET}.{table_name}"
        logger.info(f"Uploading logs to table {table_id}")
        credentials = BigQueryClass.get_google_creds()
        try:
            gbq.to_gbq(
                logs_df,
                destination_table=table_id,
                project_id=Configs.BIGQUERY_PROJECT_ID,
                if_exists="append",
                progress_bar=False,
                credentials=credentials,
                table_schema=[
                    {"name": "Logs", "type": "STRING"},
                    {"name": "Log_timestamp", "type": "TIMESTAMP"},
                ],
            )
        except Exception as e:
            logger.error(e)


class BaseFolderClass:
    def __init__(
        self,
        name: str = None,
        folder: str = None,
        dim: str = None
    ):
        self.name = name
        self.folder = folder
        self.dim = dim
        self.logger = logging.getLogger()
        self.text_logs = []

        self.initialize()

    def split_dataframe(self, df: pd.DataFrame, num_partitions: int, shuffle: bool=True):
        """
        to partition by number of extractors
        NOTE : extractors should always be same count as number of futures
        """
        indexes = df.index.tolist()
        if shuffle:
            np.random.shuffle(indexes)
        list_indexes_splited = np.array_split(indexes, num_partitions)

        list_df_splited = [
            df.iloc[indexes]
            for indexes in list_indexes_splited
        ]
        return list_df_splited

    def initialize(self):
        self.cleanup_tmp_tables_bigquery()

        table_name = BigQueryClass.get_bigquery_table_name(self.name, is_temp=False)
        self.folder_output = os.path.join(Configs.TMP_FOLDER, table_name)
        os.mkdir(self.folder_output)

    def cleanup_tmp_tables_bigquery(self):
        self.logger.warning("Cleaning up some temporary tables on Bigquery")
        table_name = BigQueryClass.get_bigquery_table_name(self.name, is_temp=True)
        table_id = f"{Configs.BIGQUERY_PROJECT_ID}.{Configs.BIGQUERY_DATASET}.{table_name}"

        self.logger.warning(f"DELETING table `{table_id}`")

        client = BigQueryClass.get_client_bigquery()
        client.delete_table(table_id, not_found_ok=True)

        self.logger.info(f"Already dropped table `{table_id}`")

    @staticmethod
    def create_session(user_email, user_password, user_otp):
        """
        Function creates a new session if the previous session expires.
        Returns a session object.
        """
        session = ReceiptBankSession(user_email, user_password, user_otp)
        session.login_receipt_bank()
        return session


    @staticmethod
    def cols_to_explode(param_folder: str, param_dim: str) -> list:
        cols_to_explode = [
            "suggestedBankTransactionMatches_nodes",
            "dataExtractionErrors",
            "expenseReports",
        ]

        if param_folder == "inbox" and param_dim == "costs":
            cols_to_explode.append("supplier_integrations")

        elif param_folder == "archive" and param_dim == "costs":
            cols_to_explode.append("supplier_integrations")

        return cols_to_explode


class CompanyListFolder(BaseFolderClass):
    def __init__(self):
        super().__init__(name="cies")
        self.login_email = Configs.RECEIPT_BANK_USER
        self.login_password = Configs.RECEIPT_BANK_PW
        self.login_secret_key = Configs.RECEIPT_BANK_SECRET_KEY
        self.full_client_list = None
        self.df_companies = None

    def get_client_list(self):
        """
        Function retrieves entire client list from Dext and 3 other lists - to be passed into extract_from_folders() function.
        Returns:
            full_client_list - key being company name, and value being json object
        """
        self.logger.info("Starting extraction of client list...")
        start = timeit.default_timer()

        headers = {"Referer": Configs.CLIENT_URL}
        client_list_param = {
            "operationName": "ClientViewQuery",
            "variables": {},
            # added 2024-04-01
            "order": "DEFAULT",
            "reversed": False,
            "query": "query ClientViewQuery($practiceId: ID!,$onlyStarred: Boolean, $filters: ClientViewMetricsFilter, $order: ClientViewMetricsOrder, $reversed: Boolean) {   viewer {     user {       fullName       __typename     }     __typename   }   practice(id: $practiceId) {     id     countryCode     permissions {       hasClientView       managerOfViewedAccount       canAddPrepareClient       canViewInXero       __typename     }     selfAssessmentEligibleClients {       totalCount       __typename     }     clientView {       id       hasStarredClients       metrics(filters: $filters, order: $order, reversed: $reversed) {         nodes {           id           forOwnAccount           accountId           accountName           accountCrn           activeMobileUsers           totalUsers           nextDeadline           deadlinePeriod           autopublishedSuppliers           itemDelay           costsItems           salesItems           expenseReports           latestItemDate           oldestItemDate           missingPaperwork           bulkRequestPaperwork           starred           practiceCode           integrationLink {             url             integration             __typename           }           __typename         }         pageInfo {           endCursor           hasNextPage           __typename         }         totalCount         __typename       }       __typename     }     users {       edges {         node {           id           fullName           __typename         }         __typename       }       pageInfo {         hasNextPage         endCursor         __typename       }       totalCount       __typename     }     __typename   } }"
            
        }
        client_list_param["variables"]["practiceId"] = Configs.PRACTICE_ID
        client_list_param["variables"]["onlyStarred"] = False

        self.full_client_list = []
        hasNextPage = True
        isRecreateSession = True
        currentPage = 1
        session = None
        count_failed_requests = 0

        while hasNextPage:
            if isRecreateSession:
                session = BaseFolderClass.create_session(self.login_email, self.login_password, self.login_secret_key)
                isRecreateSession = False
                self.logger.info(f"Created new session for page {currentPage}")

            r_client_list = session.post(
                Configs.GRAPH_URL, headers=headers, json=json.dumps(client_list_param)
            )
            print(r_client_list.status_code)
            print(r_client_list.text)
            if r_client_list.status_code != 200:
                self.logger.warning(f"Created new session failed, recreating...")
                count_failed_requests += 1
                if count_failed_requests >= Configs.MAX_FAILED_REQUESTS:
                    # raise Exception(f"Max failed requests reached")
                    self.logger.error("Max failed requests reached")
                    self.text_logs.append(f"Requesting failed, ceasing loop... for page {currentPage}")
                    break

                isRecreateSession = True
                sleep(Configs.NUM_SECOND_SLEEP_TO_RECREATE_SESSION)
                continue

            json_client_list = r_client_list.json()
            cie_json = json_client_list["data"]["clientView"]["metrics"]["nodes"]

            self.full_client_list.extend(cie_json)
            endCursor = json_client_list["data"]["clientView"]["metrics"]["pageInfo"][
                "endCursor"
            ]
            hasNextPage = json_client_list["data"]["clientView"]["metrics"]["pageInfo"][
                "hasNextPage"
            ]
            client_list_param["variables"]["after"] = endCursor

            if currentPage % 5 == 0:
                self.logger.info(f"Completed {currentPage} pages")
            currentPage += 1
            count_failed_requests = 0

            ######################
            # if currentPage >= 10:
            #     break

        end = timeit.default_timer()
        self.logger.info(
            f"Succesfully completed extracting client list from Dext! Time elapased: {(end-start)/60}! Total length {len(self.full_client_list)}"
        )

    def convert_json_to_dataframe(self):
        self.df_companies = pd.json_normalize(self.full_client_list)
        self.df_companies["time_extracted"] = datetime.utcnow()

        # export to parquet file
        if len(self.df_companies) > 0:
            export_file_path = os.path.join(self.folder_output, "dataframe_companies_cies.parquet")
            self.df_companies.to_parquet(export_file_path, index=False, engine="pyarrow", compression=None)
        else:
            self.text_logs.append(f"Skipping {self.folder} {self.dim} was empty")


    def start_process(self):
        self.get_client_list()
        self.convert_json_to_dataframe()

        text_result = BigQueryClass.upload_to_bigquery(
            output_folder=self.folder_output,
            param_folder_name=self.name
        )
        self.text_logs.append(text_result)


class OtherFolders(BaseFolderClass):
    def __init__(
        self,
        name: str = None,
        folder: str = None,
        dim: str = None
    ):
        super().__init__(name=name, folder=folder, dim=dim)

    @staticmethod
    def get_from_dext(
        df_company: pd.DataFrame,
        user_email: str,
        user_password: str,
        thread_index: int,
        param_folder: str,
        param_dim: str,
    ) -> Tuple[dict, Tuple]:
        text_logs = []

        logger = logging.getLogger()
        start = timeit.default_timer()
        logger.info(f"Thread {thread_index}: Start getting data from dext service, email: {user_email}")

        param = OtherFolders.create_new_param(param_folder, param_dim)

        isRecreateSession = True
        session = None
        full_dict = {}
        count_failed_requests = 0
        count_success_requests = 0
        for _, row in df_company.iterrows():
            acctid = row["accountId"]
            accountCrn = row["accountCrn"]
            cie_name = row["accountName"]

            headers = {
                "Referer": f"https://app.dext.com/clients/{accountCrn}/gamma/{param_dim}/{param_folder}"
            }
            param["variables"]["accountId"] = acctid

            hasNextPage = True
            while hasNextPage:
                if isRecreateSession:
                    session = BaseFolderClass.create_session(user_email, user_password)
                    isRecreateSession = False
                    logger.info(f"Thread {thread_index}: Created new session for `{user_email}`")

                try:
                    request_res = session.post(Configs.GRAPH_URL, headers=headers, json=param)
                    request_status_code = request_res.status_code
                except Exception as e:
                    logger.warning(
                        f"Thread {thread_index}: Error while sending request, message {e}"
                    )
                    request_status_code = -1

                if request_status_code == 500:
                    # No access to view this account
                    text_logs.append(f"Requesting failed, skipping... {cie_name, accountCrn}")
                    logger.warning(
                        f"Thread {thread_index}: Status code {request_status_code} for accountCrn {accountCrn} acctid {acctid}, message {request_res.json()}, ignoring this one"
                    )
                    isRecreateSession = False
                    hasNextPage = False
                    break

                elif request_status_code != 200:
                    logger.warning(
                        f"Thread {thread_index}: Status code {request_res.status_code}, message {request_res.json()}"
                    )
                    count_failed_requests += 1
                    logger.warning(f"Thread {thread_index}: Created new session failed, count failed {count_failed_requests}, recreating...")

                    if count_failed_requests >= Configs.MAX_FAILED_REQUESTS:
                        raise Exception(f"Max failed requests reached")

                    isRecreateSession = True
                    sleep(Configs.NUM_SECOND_SLEEP_TO_RECREATE_SESSION)
                    continue

                count_failed_requests = 0
                json_result = request_res.json()
                      
                json_list = json_result["data"]["account"]["receipts"]["edges"]

                if accountCrn in full_dict:
                    full_dict[accountCrn].extend(json_list)
                else:
                    full_dict[accountCrn] = json_list

                logger.info(f"Thread {thread_index}: Len of full_dict[{accountCrn}] {len(full_dict[accountCrn])}")

                pageInfo = json_result["data"]["account"]["receipts"]["pageInfo"]
                endCursor = pageInfo["endCursor"]
                hasNextPage = pageInfo["hasNextPage"]

                param["variables"]["after"] = endCursor
                # break

            logger.info(f"Thread {thread_index}: Len of full_dict {len(full_dict)}")

            count_success_requests += 1
            # if count_success_requests >= 15:
            #     break

        end = timeit.default_timer()
        logger.info(
            f"Thread {thread_index}: Succesfully converted folder: `{param_folder}`, dim: `{param_dim}` to dictionary! Time elapased: {(end-start)/60}"
        )
        return full_dict, text_logs

    @staticmethod
    def convert_json_to_dataframe(
        dict_data: dict,
        df_company: pd.DataFrame,
        thread_index: int,
        **params_folder
    ) -> Tuple[pd.DataFrame, List] :
        text_logs = []

        logger = logging.getLogger()
        results = []
        param_folder = params_folder.get("param_folder", None)
        param_dim = params_folder.get("param_dim", None)
        param_folder_name = params_folder.get("param_folder_name", None)

        for accountCrn in dict_data:
            for record in dict_data[accountCrn]:
                record["node"]["accountCrn"] = accountCrn
                results.append(record["node"])

        logger.info(f"Thread {thread_index}: len results {len(results)}")
        df = pd.json_normalize(results, sep="_")
        if len(df) == 0:
            # return schema
            text_logs.append(f"Skipping {param_folder} {param_dim} was empty")
            logger.info(f"Thread {thread_index}: dataframe is empty")
            return df.iloc[:0].copy(), text_logs

        df_company_ref = df_company[["accountName", "accountCrn"]]
        df = pd.merge(df_company_ref, df, on="accountCrn", how="right")

        df = df.rename(columns={"accountName": "company_name"})

        cols_to_explode = BaseFolderClass.cols_to_explode(param_folder=param_folder, param_dim=param_dim)
        for col in cols_to_explode:
            df = df.explode(col)

        df["time_extracted"] = datetime.utcnow()

        logger.info(f"Thread {thread_index}: Converted to dataframe, total records: {len(df)}")

        table_name = BigQueryClass.get_bigquery_table_name(param_folder_name, is_temp=False)
        folder_output = os.path.join(Configs.TMP_FOLDER, table_name)
        export_file_path = os.path.join(folder_output, f"extract_from_folder_thread{thread_index}.parquet")
        df.to_parquet(export_file_path, index=False, engine="pyarrow", compression=None)
        logger.info(f"Thread {thread_index}: Exported to `{export_file_path}`")

        return df.iloc[:0].copy(), text_logs

    @staticmethod
    def create_new_param(folder, dim):
        if folder == "inbox" and dim == "costs":
            param = {
                "operationName": "FetchMoreCosts",
                "variables": {
                    "section": "INBOX",
                    "filter": {},
                    "order": "DEFAULT",
                    "reversed": False,
                },
                "query":	"""
                  query FetchMoreCosts(
                    $accountId: ID!,
                    $after: String,
                    $section: ReceiptStatus!,
                    $filter: ReceiptFilter,
                    $order: ReceiptOrder,
                    $reversed: Boolean
                  ) {
                    account(
                      id: $accountId
                    ) {
                      id
                      receipts(
                        first: 50,
                        section: $section,
                        after: $after,
                        filter: $filter,
                        order: $order,
                        reversed: $reversed
                      ) {
                        edges {
                          node {
                            ...ReceiptFragment
                            __typename
                          }
                          __typename
                        }
                        pageInfo {
                          endCursor
                          hasNextPage
                          __typename
                          }
                        __typename
                        }
                        __typename
                        }
                      }
                      fragment
                      ReceiptFragment
                      on
                      Receipt {
                        description
                        id
                        archived
                        readyForExport
                        displaySection
                        costsKind
                        salesKind
                        ledger
                        date
                        dueDate
                        code
                        totalAmount
                        taxAmount
                        baseTotalAmount
                        baseTaxAmount
                        primaryTaxAmount
                        secondaryTaxAmount
                        hasSecondaryTax
                        invoiceNumber
                        netAmount
                        note
                        imageUrl2
                        imageContentType
                        downloadUrl
                        flagged
                        createdAt
                        exportedOn
                        euGoodsServices
                        exporterName
                        archivedOn
                        archiverName
                        unarchivedOn
                        unarchiverName
                        editedOn
                        editorName
                        currencyCode
                        integration
                        integrationDisplayName
                        read
                        isPublishable
                        publishingStatus2
                        publishingError
                        publishedOn2
                        receivedVia
                        inExpenseReport
                        isMerged
                        dataExtractionErrors
                        rebillableToClient
                        manuallyCreated
                        manuallyCreatedSalesInvoice
                        supplierTaxNumber {
                          id
                          value
                          status
                          gstRegistered
                          __typename
                        }
                        automaticMerges {
                          totalCount
                          __typename
                        }
                        potentialDuplicateOf {
                          id
                          __typename
                        }
                        supplierCategory {
                          id
                          name
                          __typename
                        }
                        customerCategory {
                          id
                          name
                          __typename
                        }
                        splitSource {
                          id
                          ledger
                          archived
                          __typename
                        }
                        partnerPermalink
                        conversation {
                          id
                          unreadCount
                          lastReadMessageId
                          __typename
                        }
                        validationData {
                          id
                          hasErrors
                          category
                          supplier
                          customer
                          date
                          dueDate
                          totalAmount
                          taxAmount
                          tax
                          lineItems
                          paymentMethod
                          currencyCode
                          salesKind
                          costsKind
                          product
                          project
                          project2
                          integration
                          invoiceNumber
                          paid
                          __typename
                        }
                        paymentMethod {
                          id
                          displayName
                          reference
                          bankAccounts {
                            id
                            name
                            integration
                            __typename
                          }
                          __typename
                        }  category {
                          id
                          displayName
                          __typename
                        }
                        categorySuggestion {
                          id
                          displayName
                          __typename
                        }
                        autocategorizedCategory {
                          id
                          __typename
                        }
                        tax {
                          id
                          name
                          rate
                          reversible
                          __typename
                        }
                        customer {
                          id
                          name
                          email
                          integrations {
                            identifier
                            __typename
                          }
                          countryCode
                          __typename
                        }
                        project {
                          id
                          displayName
                          __typename
                        }
                        project2 {
                          id
                          displayName
                          __typename
                        }
                        supplier {
                          id
                          name
                          integrationName
                          hasTax
                          integrations {
                            identifier
                            __typename
                          }
                          taxNumber
                          countryCode
                          __typename
                        }
                        documentOwner {
                          id
                          displayName
                          user {
                            usingMobileApp
                            __typename
                          }
                          __typename
                        }
                        uploadingUser {
                          id
                          fullName
                          __typename
                        }
                        product {
                          id
                          displayName
                          __typename
                        }
                        expenseReports {
                          id
                          code
                          name
                          date
                          archived
                          documentOwner {
                            id
                            displayName
                            __typename
                          }
                          __typename
                        }
                        paperworkMatch {
                          id
                          amount
                          currencyCode
                          date
                          integrationName
                          reference
                          supplierName
                          __typename
                        }
                        assignedBankTransactionMatch {
                          id
                          amount
                          bankAccountName
                          date
                          description
                          currencyCode
                          integrationName
                          __typename
                        }
                        suggestedBankTransactionMatches:
                        bankTransactionMatches(first: 2) {
                          totalCount
                          nodes {
                            id
                            amount
                            bankAccountName
                            bankName
                            date
                            description
                            currencyCode
                            integrationName
                            integrationMatch
                            __typename
                          }
                          __typename
                        }
                        backup {
                          state
                          failureReason
                          __typename
                        }
                        __typename
                      }
                  """
            }
        elif folder == "inbox" and dim == "sales":
            param = {
                "operationName": "MoreSalesQuery",
                "variables": {
                    "section": "INBOX",
                    "filter": {},
                    "order": "DEFAULT",
                    "reversed": False,
                },
                "query":	"query MoreSalesQuery($accountId: ID!, $after: String, $section: ReceiptStatus!, $filter: ReceiptFilter, $order: ReceiptOrder, $reversed: Boolean) {  account(id: $accountId) {    id    receipts(      first: 50      ledger: SALES      section: $section      after: $after      filter: $filter      order: $order      reversed: $reversed    ) {      edges {        node {          ...ReceiptFragment          __typename        }        __typename      }      pageInfo {        endCursor        hasNextPage        __typename      }      __typename    }    __typename  }}fragment ReceiptFragment on Receipt {  description  id  archived  readyForExport  displaySection  costsKind  salesKind  ledger  date  dueDate  code  totalAmount  taxAmount  baseTotalAmount  baseTaxAmount  primaryTaxAmount  secondaryTaxAmount  hasSecondaryTax  invoiceNumber  netAmount  note  imageUrl2  imageContentType  downloadUrl  flagged  createdAt  exportedOn  euGoodsServices  exporterName  archivedOn  archiverName  unarchivedOn  unarchiverName  editedOn  editorName  currencyCode  integration  integrationDisplayName  read  isPublishable  publishingStatus2  publishingError  publishedOn2  receivedVia  inExpenseReport  isMerged  dataExtractionErrors  rebillableToClient  manuallyCreated  manuallyCreatedSalesInvoice  supplierTaxNumber {    id    value    status    gstRegistered    __typename  }  automaticMerges {    totalCount    __typename  }  potentialDuplicateOf {    id    __typename  }  supplierCategory {    id    name    __typename  }  customerCategory {    id    name    __typename  }  splitSource {    id    ledger    archived    __typename  }  partnerPermalink  conversation {    id    unreadCount    lastReadMessageId    __typename  }  validationData {    id    hasErrors    category    supplier    customer    date    dueDate    totalAmount    taxAmount    tax    lineItems    paymentMethod    currencyCode    salesKind    costsKind    product    project    project2    integration    invoiceNumber    paid    __typename  }  paymentMethod {    id    displayName    reference    bankAccounts {      id      name      integration      __typename    }    __typename  }  category {    id    displayName    __typename  }  categorySuggestion {    id    displayName    __typename  }  autocategorizedCategory {    id    __typename  }  tax {    id    name    rate    reversible    __typename  }  customer {    id    name    email    integrations {      identifier      __typename    }    countryCode    __typename  }  project {    id    displayName    __typename  }  project2 {    id    displayName    __typename  }  supplier {    id    name    integrationName    hasTax    integrations {      identifier      __typename    }    taxNumber    countryCode    __typename  }  documentOwner {    id    displayName    user {      usingMobileApp      __typename    }    __typename  }  uploadingUser {    id    fullName    __typename  }  product {    id    displayName    __typename  }  expenseReports {    id    code    name    date    archived    documentOwner {      id      displayName      __typename    }    __typename  }  paperworkMatch {    id    amount    currencyCode    date    integrationName    reference    supplierName    __typename  }  assignedBankTransactionMatch {    id    amount    bankAccountName    date    description    currencyCode    integrationName    __typename  }  suggestedBankTransactionMatches: bankTransactionMatches(first: 2) {    totalCount    nodes {      id      amount      bankAccountName      bankName      date      description      currencyCode      integrationName      integrationMatch      __typename    }    __typename  }  backup {    state    failureReason    __typename  }  __typename}"
           }
        elif folder == "archive" and dim == "costs":
            param = {
                "operationName": "FetchMoreCosts",
                "variables": {
                    "section": "ARCHIVE",
                    "filter": {},
                    "order": "DEFAULT",
                    "reversed": False,
                },
                "query":	"query FetchMoreCosts($accountId: ID!, $after: String, $section: ReceiptStatus!, $filter: ReceiptFilter, $order: ReceiptOrder, $reversed: Boolean) {  account(id: $accountId) {    id    receipts(      first: 50      section: $section      after: $after      filter: $filter      order: $order      reversed: $reversed    ) {      edges {        node {          ...ReceiptFragment          __typename        }        __typename      }      pageInfo {        endCursor        hasNextPage        __typename      }      __typename    }    __typename  }}fragment ReceiptFragment on Receipt {  description  id  archived  readyForExport  displaySection  costsKind  salesKind  ledger  date  dueDate  code  totalAmount  taxAmount  baseTotalAmount  baseTaxAmount  primaryTaxAmount  secondaryTaxAmount  hasSecondaryTax  invoiceNumber  netAmount  note  imageUrl2  imageContentType  downloadUrl  flagged  createdAt  exportedOn  euGoodsServices  exporterName  archivedOn  archiverName  unarchivedOn  unarchiverName  editedOn  editorName  currencyCode  integration  integrationDisplayName  read  isPublishable  publishingStatus2  publishingError  publishedOn2  receivedVia  inExpenseReport  isMerged  dataExtractionErrors  rebillableToClient  manuallyCreated  manuallyCreatedSalesInvoice  supplierTaxNumber {    id    value    status    gstRegistered    __typename  }  automaticMerges {    totalCount    __typename  }  potentialDuplicateOf {    id    __typename  }  supplierCategory {    id    name    __typename  }  customerCategory {    id    name    __typename  }  splitSource {    id    ledger    archived    __typename  }  partnerPermalink  conversation {    id    unreadCount    lastReadMessageId    __typename  }  validationData {    id    hasErrors    category    supplier    customer    date    dueDate    totalAmount    taxAmount    tax    lineItems    paymentMethod    currencyCode    salesKind    costsKind    product    project    project2    integration    invoiceNumber    paid    __typename  }  paymentMethod {    id    displayName    reference    bankAccounts {      id      name      integration      __typename    }    __typename  }  category {    id    displayName    __typename  }  categorySuggestion {    id    displayName    __typename  }  autocategorizedCategory {    id    __typename  }  tax {    id    name    rate    reversible    __typename  }  customer {    id    name    email    integrations {      identifier      __typename    }    countryCode    __typename  }  project {    id    displayName    __typename  }  project2 {    id    displayName    __typename  }  supplier {    id    name    integrationName    hasTax    integrations {      identifier      __typename    }    taxNumber    countryCode    __typename  }  documentOwner {    id    displayName    user {      usingMobileApp      __typename    }    __typename  }  uploadingUser {    id    fullName    __typename  }  product {    id    displayName    __typename  }  expenseReports {    id    code    name    date    archived    documentOwner {      id      displayName      __typename    }    __typename  }  paperworkMatch {    id    amount    currencyCode    date    integrationName    reference    supplierName    __typename  }  assignedBankTransactionMatch {    id    amount    bankAccountName    date    description    currencyCode    integrationName    __typename  }  suggestedBankTransactionMatches: bankTransactionMatches(first: 2) {    totalCount    nodes {      id      amount      bankAccountName      bankName      date      description      currencyCode      integrationName      integrationMatch      __typename    }    __typename  }  backup {    state    failureReason    __typename  }  __typename}"
            }
        elif folder == "archive" and dim == "sales":
            param = {
                "operationName": "MoreSalesQuery",
                "variables": {
                    "section": "ARCHIVE",
                    "filter": {},
                    "order": "DEFAULT",
                    "reversed": False,
                },
                "query":	"query MoreSalesQuery($accountId: ID!, $after: String, $section: ReceiptStatus!, $filter: ReceiptFilter, $order: ReceiptOrder, $reversed: Boolean) {  account(id: $accountId) {    id    receipts(      first: 50      ledger: SALES      section: $section      after: $after      filter: $filter      order: $order      reversed: $reversed    ) {      edges {        node {          ...ReceiptFragment          __typename        }        __typename      }      pageInfo {        endCursor        hasNextPage        __typename      }      __typename    }    __typename  }}fragment ReceiptFragment on Receipt {  description  id  archived  readyForExport  displaySection  costsKind  salesKind  ledger  date  dueDate  code  totalAmount  taxAmount  baseTotalAmount  baseTaxAmount  primaryTaxAmount  secondaryTaxAmount  hasSecondaryTax  invoiceNumber  netAmount  note  imageUrl2  imageContentType  downloadUrl  flagged  createdAt  exportedOn  euGoodsServices  exporterName  archivedOn  archiverName  unarchivedOn  unarchiverName  editedOn  editorName  currencyCode  integration  integrationDisplayName  read  isPublishable  publishingStatus2  publishingError  publishedOn2  receivedVia  inExpenseReport  isMerged  dataExtractionErrors  rebillableToClient  manuallyCreated  manuallyCreatedSalesInvoice  supplierTaxNumber {    id    value    status    gstRegistered    __typename  }  automaticMerges {    totalCount    __typename  }  potentialDuplicateOf {    id    __typename  }  supplierCategory {    id    name    __typename  }  customerCategory {    id    name    __typename  }  splitSource {    id    ledger    archived    __typename  }  partnerPermalink  conversation {    id    unreadCount    lastReadMessageId    __typename  }  validationData {    id    hasErrors    category    supplier    customer    date    dueDate    totalAmount    taxAmount    tax    lineItems    paymentMethod    currencyCode    salesKind    costsKind    product    project    project2    integration    invoiceNumber    paid    __typename  }  paymentMethod {    id    displayName    reference    bankAccounts {      id      name      integration      __typename    }    __typename  }  category {    id    displayName    __typename  }  categorySuggestion {    id    displayName    __typename  }  autocategorizedCategory {    id    __typename  }  tax {    id    name    rate    reversible    __typename  }  customer {    id    name    email    integrations {      identifier      __typename    }    countryCode    __typename  }  project {    id    displayName    __typename  }  project2 {    id    displayName    __typename  }  supplier {    id    name    integrationName    hasTax    integrations {      identifier      __typename    }    taxNumber    countryCode    __typename  }  documentOwner {    id    displayName    user {      usingMobileApp      __typename    }    __typename  }  uploadingUser {    id    fullName    __typename  }  product {    id    displayName    __typename  }  expenseReports {    id    code    name    date    archived    documentOwner {      id      displayName      __typename    }    __typename  }  paperworkMatch {    id    amount    currencyCode    date    integrationName    reference    supplierName    __typename  }  assignedBankTransactionMatch {    id    amount    bankAccountName    date    description    currencyCode    integrationName    __typename  }  suggestedBankTransactionMatches: bankTransactionMatches(first: 2) {    totalCount    nodes {      id      amount      bankAccountName      bankName      date      description      currencyCode      integrationName      integrationMatch      __typename    }    __typename  }  backup {    state    failureReason    __typename  }  __typename}"
            }
        return param

    @staticmethod
    def extract_from_folder(
        task_params: tuple
    ) -> Tuple[pd.DataFrame, List]:
        (
            df_company,
            user_email,
            user_password,
            thread_index,
            params_folder
        ) = task_params

        text_logs = []

        full_dict_extracted, return_text_logs = OtherFolders.get_from_dext(
            df_company=df_company,
            user_email=user_email,
            user_password=user_password,
            thread_index=thread_index,
            param_folder=params_folder.get("folder", None),
            param_dim=params_folder.get("dim", None),
        )
        text_logs.extend(return_text_logs)

        df_schema, return_text_logs = OtherFolders.convert_json_to_dataframe(
            dict_data=full_dict_extracted,
            df_company=df_company,
            thread_index=thread_index,
            param_folder=params_folder.get("folder", None),
            param_dim=params_folder.get("dim", None),
            param_folder_name=params_folder.get("name", None),
        )
        text_logs.extend(return_text_logs)

        return df_schema, text_logs


    def start_process(self, df_company: pd.DataFrame):
        self.logger.info(f"Starting task `{self.name}`")
        start_time = timeit.default_timer()

        df_company = df_company.rename(columns={"company_name": "accountName"})
        list_companies_splited = self.split_dataframe(df_company, Configs.NUM_THREADING)

        params_folder = {
            "name": self.name,
            "folder": self.folder,
            "dim": self.dim
        }

        task_params = [
            (
                list_companies_splited[thread_index],
                Configs.user_creds_list[thread_index][0],
                Configs.user_creds_list[thread_index][1],
                thread_index,
                params_folder,
            )
            for thread_index in range(Configs.NUM_THREADING)
        ]

        df_schema = pd.DataFrame()
        with concurrent.futures.ThreadPoolExecutor(max_workers=Configs.NUM_THREADING) as executor:
            for schema_result, return_text_logs in executor.map(OtherFolders.extract_from_folder, task_params):
                df_schema = pd.concat([df_schema, schema_result], axis=0, ignore_index=True)
                self.text_logs.extend(return_text_logs)

        text_result = BigQueryClass.upload_to_bigquery(
            output_folder=self.folder_output,
            param_folder_name=self.name,
            df_schema=df_schema,
        )
        self.text_logs.append(text_result)

        stop_time = timeit.default_timer()
        self.logger.info(f"Task `{self.name}` completed in {stop_time - start_time} seconds")


class PublishHistFolder(BaseFolderClass):
    def __init__(self):
        super().__init__(name="archived_costs_publish_info", folder=None, dim=None)

    def get_lastest_datetime_in_bigquery(self):
        table_name = BigQueryClass.get_bigquery_table_name(param_folder_name="archived_costs", is_temp=False)
        sql = f"""
            SELECT DISTINCT
                PARSE_TIMESTAMP('%F %T',LEFT(time_extracted,STRPOS(time_extracted,'.')-1)) time_extracted
            FROM `{Configs.BIGQUERY_PROJECT_ID}.{Configs.BIGQUERY_DATASET}.{table_name}`
        """
        df = gbq.read_gbq(sql, progress_bar_type=None)
        latest_time = df["time_extracted"]
        latest_time = latest_time[0].date()
        return latest_time

    def get_archived_costs_df(self, latest_time: str = None):
        table_name = BigQueryClass.get_bigquery_table_name(param_folder_name="archived_costs", is_temp=False)
        self.logger.info(f"Fetching data from table `{table_name}`...")

        sql = f"""
            SELECT
                SAFE.PARSE_DATE('%F', publishedOn2) publishedOn2,
                company_name,
                id
            FROM `{Configs.BIGQUERY_PROJECT_ID}.{Configs.BIGQUERY_DATASET}.{table_name}`
        """
        if latest_time:
            where_condition = f"""
                WHERE SAFE.PARSE_DATE('%F', publishedOn2) >= '{latest_time}'
            """
        else:
            where_condition = ""
        sql += where_condition

        self.logger.info(sql)

        df = gbq.read_gbq(sql, progress_bar_type=None)
        self.logger.info(f"Fetched {len(df)} records from table `{table_name}`")
        return df

    @staticmethod
    def get_from_dext(
        df_archive_costs: pd.DataFrame,
        user_email: str,
        user_password: str,
        thread_index: int
    ) -> Tuple[dict, List]:
        text_logs = []
        logger = logging.getLogger()
        start_time = timeit.default_timer()
        logger.info(f"Thread {thread_index}: Start getting data from dext service, email: {user_email}")

        session = None
        full_dict = {}
        count_failed_requests = 0
        isRecreateSession = True
        for index, row in df_archive_costs.iterrows():
            cie_name = row["company_name"]
            receipt_id = row["id"]

            request_params = {
                "operationName": "ReceiptHistoryQuery",
                "variables": {},
                "query": "query ReceiptHistoryQuery($id: ID!) {  receiptHistoryEntries(receiptId: $id) {    nodes {      id      action      description      createdAt      userName      __typename    }    __typename  }}",
            }
            request_params["variables"]["id"] = row["id"]

            response_status_code = -1
            while response_status_code != 200:
                if isRecreateSession:
                    session = BaseFolderClass.create_session(user_email, user_password)
                    isRecreateSession = False
                    logger.info(f"Thread {thread_index}: Created new session for `{user_email}`")

                try:
                    request_res = session.post(Configs.GRAPH_URL, json=request_params)
                    response_status_code = request_res.status_code
                except Exception as e:
                    logger.warning(
                        f"Thread {thread_index}: Error while sending request, message {e}"
                    )
                    response_status_code = -1

                if response_status_code == 500:
                    logger.warning(
                        f"Thread {thread_index}: Status code {request_res.status_code} for receipt_id {receipt_id}, message {request_res.json()}, ignoring this one"
                    )
                    text_logs.append(f"Skipping over {cie_name}, reciept_id {receipt_id}, index: {index}")
                    break

                elif response_status_code != 200:
                    logger.warning(
                        f"Thread {thread_index}: Status code {request_res.status_code}, message {request_res.json()}"
                    )
                    logger.warning(f"Thread {thread_index}: Request error code {request_res.status_code}, recreating...")
                    count_failed_requests += 1
                    if count_failed_requests >= Configs.MAX_FAILED_REQUESTS:
                        raise Exception(f"Max failed requests reached")

                    isRecreateSession = True
                    sleep(Configs.NUM_SECOND_SLEEP_TO_RECREATE_SESSION)
                else:
                    count_failed_requests = 0

            if response_status_code == 200:
                # Only update if response_status_code = 200
                json_result = request_res.json()
                json_list = json_result["data"]["receiptHistoryEntries"]["nodes"]

                if cie_name in full_dict:
                    full_dict[cie_name][receipt_id] = json_list
                else:
                    full_dict[cie_name] = {}
                    full_dict[cie_name][receipt_id] = json_list

        end_time = timeit.default_timer()
        logger.info(
            f"Thread {thread_index}: Succesfully converted publishing history to dictionary! Time elapased: {(end_time - start_time)/60}"
        )
        return full_dict, text_logs

    @staticmethod
    def convert_json_to_dataframe(
        dict_data: dict,
        thread_index: int,
        **params_folder
    ) -> Tuple[pd.DataFrame, List]:
        text_logs = []

        logger = logging.getLogger()
        param_folder_name = params_folder.get("param_folder_name", None)
        results = []
        for company in dict_data:
            for receipt in dict_data[company]:
                for log in dict_data[company][receipt]:
                    log["company_name"] = company
                    log["receipt_id"] = receipt
                    results.append(log)

        logger.info(f"Thread {thread_index}: len results {len(results)}")
        df = pd.json_normalize(results)
        df["time_extracted"] = datetime.utcnow()
        df["accountName"] = ''

        if len(df) == 0:
            # return schema
            text_logs.append(f"Skipping {param_folder_name} was empty")
            logger.info(f"Thread {thread_index}: dataframe is empty")
            return df.iloc[:0].copy(), text_logs

        table_name = BigQueryClass.get_bigquery_table_name(param_folder_name, is_temp=False)
        folder_output = os.path.join(Configs.TMP_FOLDER, table_name)
        export_file_path = os.path.join(folder_output, f"extract_from_folder_thread{thread_index}.parquet")
 
        df.to_parquet(export_file_path, index=False, engine="pyarrow", compression=None)
        logger.info(f"Thread {thread_index}: Exported to `{export_file_path}`")

        return df.iloc[:0].copy(), text_logs


    @staticmethod
    def extract_publish_hist(
        task_params: tuple
    ) -> Tuple[pd.DataFrame, List] :
        (
            df_archive_costs,
            user_email,
            user_password,
            thread_index,
            params_folder
        ) = task_params

        text_logs = []

        full_dict_extracted, return_text_logs = PublishHistFolder.get_from_dext(
            df_archive_costs=df_archive_costs,
            user_email=user_email,
            user_password=user_password,
            thread_index=thread_index
        )
        text_logs.extend(return_text_logs)

        df_schema, return_text_logs = PublishHistFolder.convert_json_to_dataframe(
            dict_data=full_dict_extracted,
            thread_index=thread_index,
            param_folder_name=params_folder.get("name", None),
        )
        text_logs.extend(return_text_logs)

        return df_schema, text_logs

    def retrieve_df_archive_costs(
        self,
        is_append=True,
        num_days_publish_hist=Configs.NUM_DAYS_PUBLISH_HIST
    ) -> pd.DataFrame:
        table_name = BigQueryClass.get_bigquery_table_name(param_folder_name="archived_costs", is_temp=False)
        folder_output = os.path.join(Configs.TMP_FOLDER, table_name)

        latest_time = self.get_lastest_datetime_in_bigquery() - timedelta(days=num_days_publish_hist)

        df = pd.DataFrame()
        for parquet_file in glob.glob(os.path.join(folder_output, "*.parquet")):
            data_df = pd.read_parquet(parquet_file)
            data_df = data_df[["publishedOn2", "company_name", "id"]]

            if len(data_df) > 0 and is_append:
                data_df = data_df[
                    pd.to_datetime(data_df["publishedOn2"]).dt.date > latest_time
                ]

            df = pd.concat([df, data_df], axis=0, ignore_index=True)

        if len(df) == 0:
            if is_append:
                df = self.get_archived_costs_df(latest_time=latest_time.strftime("%Y-%m-%d"))
            else:
                df = self.get_archived_costs_df(latest_time=None)

        return df


    def start_process(self):
        self.logger.info(f"Starting task `{self.name}`")
        start_time = timeit.default_timer()

        df_archive_costs = self.retrieve_df_archive_costs()
        list_archive_costs_dfs = self.split_dataframe(df_archive_costs, Configs.NUM_THREADING)

        params_folder = {
            "name": self.name,
            "folder": self.folder,
            "dim": self.dim
        }
        task_params = [
            (
                list_archive_costs_dfs[thread_index],
                Configs.user_creds_list[thread_index][0],
                Configs.user_creds_list[thread_index][1],
                thread_index,
                params_folder,
            )
            for thread_index in range(Configs.NUM_THREADING)
        ]

        del df_archive_costs
        gc.collect()

        df_schema = pd.DataFrame()
        with concurrent.futures.ThreadPoolExecutor(max_workers=Configs.NUM_THREADING) as executor:
            for schema_result, return_text_logs in executor.map(PublishHistFolder.extract_publish_hist, task_params):
                df_schema = pd.concat([df_schema, schema_result], axis=0, ignore_index=True)
                self.text_logs.extend(return_text_logs)

        text_result = BigQueryClass.upload_to_bigquery(
            output_folder=self.folder_output,
            param_folder_name=self.name,
            df_schema=df_schema,
            write_mode="append"
        )
        self.text_logs.append(text_result)

        # self.insert_hist_extracted()

        stop_time = timeit.default_timer()
        self.logger.info(f"Task `{self.name}` completed in {stop_time - start_time} seconds")


def initialize():
    if os.path.isdir(Configs.TMP_FOLDER):
        shutil.rmtree(Configs.TMP_FOLDER)

    os.mkdir(Configs.TMP_FOLDER)


def main():
    logger = logging.getLogger()
    time_start = timeit.default_timer()
    initialize()

    BigQueryClass.inititalize_bigquery_session()

    text_logs = []

    # # Task company_list
    company_list_folder = CompanyListFolder()
    company_list_folder.start_process()
    text_logs.extend(company_list_folder.text_logs)

    # Task archive_cost
    archive_cost_folder = OtherFolders(name="archived_costs", folder="archive", dim="costs",)
    archive_cost_folder.start_process(company_list_folder.df_companies)
    text_logs.extend(archive_cost_folder.text_logs)

    # Task archive_sale
    archive_sale_folder = OtherFolders(name="archived_sales", folder="archive", dim="sales")
    archive_sale_folder.start_process(company_list_folder.df_companies)
    text_logs.extend(archive_sale_folder.text_logs)

    # Task inbox_cost
    inbox_cost_folder = OtherFolders(name="inbox_costs", folder="inbox", dim="costs")
    inbox_cost_folder.start_process(company_list_folder.df_companies)
    text_logs.extend(inbox_cost_folder.text_logs)

    # Task inbox_sale
    inbox_sale_folder = OtherFolders(name="inbox_sales", folder="inbox", dim="sales")
    inbox_sale_folder.start_process(company_list_folder.df_companies)
    text_logs.extend(inbox_sale_folder.text_logs)

    # Task publish_hist
    publish_hist_folder = PublishHistFolder()
    publish_hist_folder.start_process()
    text_logs.extend(publish_hist_folder.text_logs)

    logs_df = pd.DataFrame(text_logs, columns=["Logs"])
    logs_df["Log_timestamp"] = datetime.now()
    logs_df.to_csv("tb_dext_directload_extraction_logs.csv")
    BigQueryClass.upload_logs_to_bigquery(logs_df=logs_df)

    time_stop = timeit.default_timer()
    logger.info(f"All tasks completed on {time_stop - time_start} seconds")


if __name__ == "__main__":
    main()