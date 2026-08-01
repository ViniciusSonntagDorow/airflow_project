import logging

import pandas as pd
import sidrapy


class SidraExtractor:
    def __init__(self, table_code:str, variable:str, classifications_key:str, territorial_level:str = "6", ibge_territorial_code:str = "all"):
        self.table_code = table_code
        self.variable = variable
        self.classifications_key = classifications_key
        self.territorial_level = territorial_level
        self.ibge_territorial_code = ibge_territorial_code
    
    def fetch_data(self, product: str, year: str) -> pd.DataFrame:
        """Fetch data for a single product."""
        try:
            classifications = {self.classifications_key: product} if self.classifications_key else {}
            df = sidrapy.get_table(
                table_code=self.table_code,
                territorial_level=self.territorial_level,
                ibge_territorial_code=self.ibge_territorial_code,
                variable=self.variable,
                classifications=classifications,
                period=year,
                header="n",
                format="pandas",
            )
            if df.empty:
                raise ValueError(f"No data found for product: {product}")
            logging.info(f"Processed product: {product}")
            return df
        except Exception as e:
            logging.error(f"Unexpected error for product {product}: {e}")
            raise