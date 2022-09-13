from restuarant.entity.config_entity import DataIngestionConfig
import sys,os
from restuarant.exception import RestuarantException
from restuarant.logger import logging
from restuarant.entity.artifact_entity import DataIngestionArtifact
import pandas as pd
from sklearn.model_selection import train_test_split
from six.moves import urllib

class DataIngestion:

    def __init__(self,data_ingestion_config:DataIngestionConfig ):
        try:
            logging.info(f"{'>>'*20}Data Ingestion log started.{'<<'*20} ")
            self.data_ingestion_config = data_ingestion_config

        except Exception as e:
            raise RestuarantException(e,sys)
    

    def download_data(self,) -> str:
        try:
            #extraction remote url to download dataset
            download_url = self.data_ingestion_config.dataset_download_url

            #folder location to download file
            csv_download_dir = self.data_ingestion_config.csv_download_dir
            
            os.makedirs(csv_download_dir,exist_ok=True)

            restuarant_file_name = os.path.basename(download_url)

            csv_file_path = os.path.join(csv_download_dir, restuarant_file_name)

            logging.info(f"Downloading file from :[{download_url}] into :[{csv_file_path}]")
            urllib.request.urlretrieve(download_url, csv_file_path)
            logging.info(f"File :[{csv_file_path}] has been downloaded successfully.")
            return csv_file_path

        except Exception as e:
            raise RestuarantException(e,sys) from e

    
    def split_data_as_train_test(self) -> DataIngestionArtifact:
        try:
            csv_data_dir = self.data_ingestion_config.csv_download_dir

            file_name = os.listdir(csv_data_dir)[0]

            zomato_file_path = os.path.join(csv_data_dir,file_name)


            logging.info(f"Reading csv file: [{zomato_file_path}]")
            zomato_dataframe = pd.read_csv(zomato_file_path)

            logging.info(f"Splitting data into train and test")

            # Train test split
            train_set, test_set = train_test_split(zomato_dataframe, test_size=0.2, random_state=1)

            train_file_path = os.path.join(self.data_ingestion_config.ingested_train_dir,
                                            file_name)

            test_file_path = os.path.join(self.data_ingestion_config.ingested_test_dir,
                                        file_name)

            if train_set is not None:
                os.makedirs(self.data_ingestion_config.ingested_train_dir,exist_ok=True)
                logging.info(f"Exporting training datset to file: [{train_file_path}]")
                train_set.to_csv(train_file_path,index=False)

            if test_set is not None:
                os.makedirs(self.data_ingestion_config.ingested_test_dir, exist_ok= True)
                logging.info(f"Exporting test dataset to file: [{test_file_path}]")
                test_set.to_csv(test_file_path,index=False)            


            data_ingestion_artifact = DataIngestionArtifact(train_file_path=train_file_path,
                                test_file_path=test_file_path,
                                is_ingested=True,
                                message=f"Data ingestion completed successfully."
                                )
            logging.info(f"Data Ingestion artifact:[{data_ingestion_artifact}]")
            return data_ingestion_artifact

        except Exception as e:
            raise RestuarantException(e,sys) from e


    def initiate_data_ingestion(self)-> DataIngestionArtifact:
        try:
            csv_file_path =  self.download_data()
            return self.split_data_as_train_test()
        except Exception as e:
            raise RestuarantException(e,sys) from e
    


    def __del__(self):
        logging.info(f"{'>>'*20}Data Ingestion log completed.{'<<'*20} \n\n")
