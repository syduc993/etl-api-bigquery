import unittest
from unittest.mock import MagicMock, patch
from src.features.nhanh.bills.extractor import BillExtractor

class TestBillExtractor(unittest.TestCase):
    def setUp(self):
        self.extractor = BillExtractor()
        # Mock dependencies if needed
        
    def test_initialization(self):
        self.assertEqual(self.extractor.platform, "nhanh")
        self.assertEqual(self.extractor.entity, "bills")

    @patch('src.shared.nhanh.client.NhanhApiClient.fetch_paginated')
    def test_extract_calls_api(self, mock_fetch):
        # Setup mock return
        mock_fetch.return_value = [{"id": 123}]
        
        # Execute
        result = self.extractor.extract()
        
        # Assert
        self.assertTrue(len(result) == 1)
        mock_fetch.assert_called_once()

if __name__ == '__main__':
    unittest.main()
