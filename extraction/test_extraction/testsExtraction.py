import unittest
from unittest.mock import MagicMock
from extraction import opt_extraction

class Test_Extract(unittest.TestCase):
    def test_get_data_Njoin(self):
        """
            Test that data in table is correctly formatted
        """
        extractor = opt_extraction.ExtractData(database_Njoin=['some_database'], database_Wjoin=['another'])
        mock_result = [
            ("value1", "value2", "value3"),
            ("value4", "value5", "value6"),
        ]

        extractor.query_mariadb = MagicMock(return_value = mock_result) # MagicMock allow to force the method query_mariadb to return mock_result

        # manually define the structure 
        extractor.dict_structure = {
            'fake_db' : {
                'table_1': ['col_1', 'col_2', 'col_3']
            }
        }

        document = {
            'fake_db': {
                'table_1': {}
            }
        }

        expected_result = {
                    'fake_db': {
                        'table_1': {
                            'item_0': {'col_1': 'value1', 'col_2': 'value2', 'col_3': 'value3'},
                            'item_1': {'col_1': 'value4', 'col_2': 'value5', 'col_3': 'value6'}
                        }
                    }
                }

        result = extractor._get_data_Njoin(document, 'fake_db', 'table_1')

        # assert statement to check that the function result and the expected one are the same
        self.assertEqual(result, expected_result)

if __name__ == '__main__':
    unittest.main()