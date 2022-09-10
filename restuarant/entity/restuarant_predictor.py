import os
import sys

from restuarant.exception import RestuarantException
from restuarant.util.util import load_object

import pandas as pd


class RestuarantData:

    def __init__(self,
                 votes: int,
                 average_cost_for_two: int,
                 has_table_booking: int,
                 has_online_delivery: int,
                 price_range: int
                 ):
        try:
            self.votes = votes
            self.average_cost_for_two = average_cost_for_two
            self.has_table_booking = has_table_booking
            self.has_online_delivery = has_online_delivery
            self.price_range = price_range

        except Exception as e:
            raise RestuarantException(e, sys) from e

    def get_restuarant_input_data_frame(self):

        try:
            restuarant_input_dict = self.get_restuarant_data_as_dict()
            return pd.DataFrame(restuarant_input_dict)
        except Exception as e:
            raise RestuarantException(e, sys) from e

    def get_restuarant_data_as_dict(self):
        try:
            input_data = {
                "Votes": [self.votes],
                "Average_Cost_for_two": [self.average_cost_for_two],
                "Has_Table_booking": [self.has_table_booking],
                "Has_Online_delivery": [self.has_online_delivery],
                "Price_range": [self.price_range]}
            return input_data
        except Exception as e:
            raise RestuarantException(e, sys)


class RestuarantPredictor:

    def __init__(self, model_dir: str):
        try:
            self.model_dir = model_dir
        except Exception as e:
            raise RestuarantException(e, sys) from e

    def get_latest_model_path(self):
        try:
            folder_name = list(map(int, os.listdir(self.model_dir)))
            latest_model_dir = os.path.join(self.model_dir, f"{max(folder_name)}")
            file_name = os.listdir(latest_model_dir)[0]
            latest_model_path = os.path.join(latest_model_dir, file_name)
            return latest_model_path
        except Exception as e:
            raise RestuarantException(e, sys) from e

    def predict(self, X):
        try:
            model_path = self.get_latest_model_path()
            model = load_object(file_path=model_path)
            median_house_value = model.predict(X)
            return median_house_value
        except Exception as e:
            raise RestuarantException(e, sys) from e