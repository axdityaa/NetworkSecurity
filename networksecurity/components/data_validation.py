from networksecurity.entity.artifact_entity import DataIngestionArtifact,DataValidationArtifact
from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.constant.training_pipeline import SCHEMA_FILE_PATH
from networksecurity.utils.main_utils.utils import read_yaml_file , write_yaml_file
import pandas as pd
import numpy as np
from scipy.stats import ks_2samp
import sys
import os   

class DataValidation:
    def __init__(self,data_ingestion_artifact:DataIngestionArtifact,
                 data_validation_config:DataValidationConfig):
        
        try:
            self.data_ingestion_artifact=data_ingestion_artifact
            self.data_validation_config=data_validation_config
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    @staticmethod
    # def fun_name(params): -> return_type
    def read_data(file_path)->pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def validate_number_of_columns(self,dataframe:pd.DataFrame)->bool:
        try:
            number_of_columns = len(self._schema_config)
            logging.info(f"Required number of columns: {number_of_columns}")
            logging.info(f"Dataframe has columns: {len(dataframe.columns)}")
            if len(dataframe.columns) == number_of_columns:
                return True
            return False
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def detect_data_drift(self, base_df, current_df, threshold=0.1) -> bool:
        try:
            status = True
            report = {}

            for column in base_df.columns:
                if column not in current_df.columns:
                    continue

                d1 = base_df[column]
                d2 = current_df[column]

                # Only numeric
                if not np.issubdtype(d1.dtype, np.number):
                    continue

                d1 = d1.dropna()
                d2 = d2.dropna()

                if len(d1) == 0 or len(d2) == 0:
                    continue

                mean_diff = abs(d1.mean() - d2.mean())
                std = d1.std() if d1.std() != 0 else 1

                drift_score = mean_diff / std

                is_found = drift_score > threshold
                if is_found:
                    status = False

                report[column] = {
                    "drift_score": float(drift_score),
                    "drift_status": is_found
                }

            drift_report_file_path = self.data_validation_config.drift_report_file_path
            os.makedirs(os.path.dirname(drift_report_file_path), exist_ok=True)
            write_yaml_file(drift_report_file_path, report)

            return status

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    ## Function to detect is numerical column exist in data
    def is_numerical_column_exist(self, dataframe: pd.DataFrame) -> bool:
        try:
            numerical_columns = self._schema_config["numerical_columns"]

            for column in numerical_columns:
                # Check column existence
                if column not in dataframe.columns:
                    return False

                # Check numerical dtype
                if not np.issubdtype(dataframe[column].dtype, np.number):
                    return False

            return True

        except Exception as e:
            raise NetworkSecurityException(e, sys)



    def intitiate_data_validation(self)->DataValidationArtifact:
        try:
            train_file_path = self.data_ingestion_artifact.train_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path
            #read the data from test file path
            test_dataframe = DataValidation.read_data(test_file_path)
            train_dataframe = DataValidation.read_data(train_file_path) 

            #validate number of columns
            status = self.validate_number_of_columns(train_dataframe)
            if not status:
                error_message = f"Train dataframe does not contain all columns"
            status = self.validate_number_of_columns(test_dataframe)
            if not status:
                error_message = f"Test dataframe does not contain all columns"

            # validate numerical columns exist
            status = self.is_numerical_column_exist(train_dataframe)
            if not status:
                error_message = f"Train dataframe does not contain numerical columns"   
            status = self.is_numerical_column_exist(test_dataframe)
            if not status:      
                error_message = f"Test dataframe does not contain numerical columns"

            # lets check data drift
            status = self.detect_data_drift(base_df=train_dataframe,current_df=test_dataframe)
            dir_path = os.path.dirname(self.data_validation_config.valid_train_file_path)
            os.makedirs(dir_path,exist_ok=True)

            if status==True:
                train_dataframe.to_csv(
                    self.data_validation_config.valid_train_file_path, index=False, header=True

                )
                test_dataframe.to_csv(
                    self.data_validation_config.valid_test_file_path, index=False, header=True
                )
                
            data_validation_artifact = DataValidationArtifact(
                validation_status=status,
                valid_train_file_path=self.data_ingestion_artifact.train_file_path,
                valid_test_file_path=self.data_ingestion_artifact.test_file_path,
                invalid_train_file_path="",
                invalid_test_file_path="",
                drift_report_file_path=self.data_validation_config.drift_report_file_path,
            )
            return data_validation_artifact

        except Exception as e:
            raise NetworkSecurityException(e,sys)