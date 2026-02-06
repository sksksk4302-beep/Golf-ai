import unittest
from unittest.mock import MagicMock, patch
import datetime
from ingest_data import save_tee_times
from app import app

class TestOptimization(unittest.TestCase):
    def setUp(self):
        self.mock_db = MagicMock()
        self.mock_batch = MagicMock()
        self.mock_db.batch.return_value = self.mock_batch
        self.mock_collection = MagicMock()
        self.mock_db.collection.return_value = self.mock_collection

    def test_save_tee_times_optimization_logic(self):
        print("\nTesting save_tee_times optimization...")
        
        # Setup
        target_date = "2025-12-25"
        
        # New Data (Crawled)
        tee_times = [
            {"golf": "ClubA", "date": target_date, "time": "08:00", "hour_num": 8, "price": 10000},
            {"golf": "ClubB", "date": target_date, "time": "09:00", "hour_num": 9, "price": 20000},
        ]
        
        # Mocking the cleanup query (which now uses range query < sync_id)
        mock_query = self.mock_collection.where.return_value.where.return_value
        mock_query.stream.return_value = [MagicMock(id="old_doc")]
        
        # Run
        save_tee_times(self.mock_db, tee_times, target_date)
        
        # Verify range query was used: .where('date','==',date).where('sync_id','<',sync_id)
        self.mock_collection.where.assert_any_call('date', '==', target_date)
        self.mock_collection.where.return_value.where.assert_any_call('sync_id', '<', unittest.mock.ANY)
        
        # Verify cleanup batch delete was called
        self.assertTrue(self.mock_batch.delete.called)
        
        # Verify upserts
        self.assertEqual(self.mock_batch.set.call_count, 2)
        
        print("save_tee_times range query verified!")

    @patch('app.db')
    def test_get_prices_batch_filtering(self, mock_db_app):
        print("\nTesting get_prices batching and filtering...")
        
        # Mock Request Data
        with app.test_request_context(json={
            "dates": ["2025-12-25"],
            "clubs": ["ClubA"],
            "times": []
        }):
            mock_daily_stats = MagicMock()
            mock_tee_times = MagicMock()
            
            def collection_side_effect(name):
                if name == 'daily_stats': return mock_daily_stats
                if name == 'tee_times': return mock_tee_times
                return MagicMock()
                
            mock_db_app.collection.side_effect = collection_side_effect
            
            # Setup mock chains for .where().where().stream()
            mock_daily_stats.where.return_value.where.return_value.stream.return_value = []
            mock_tee_times.where.return_value.where.return_value.stream.return_value = []
            
            # Run
            from app import get_prices
            get_prices()
            
            # Verify filtering: .where('date','==',date).where('club_name','in',chunk)
            mock_daily_stats.where.assert_any_call('date', '==', '2025-12-18')
            mock_daily_stats.where.return_value.where.assert_any_call('club_name', 'in', ['ClubA'])
            
            mock_tee_times.where.assert_any_call('date', '==', '2025-12-25')
            mock_tee_times.where.return_value.where.assert_any_call('club_name', 'in', ['ClubA'])
            
            print("get_prices filtered reads verified!")

if __name__ == '__main__':
    unittest.main()
