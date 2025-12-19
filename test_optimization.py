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

    def test_save_tee_times_optimization(self):
        print("\nTesting save_tee_times optimization...")
        
        # Setup
        target_date = "2025-12-25"
        
        # 1. New Data (Crawled)
        # Item 1: Same as existing
        # Item 2: Changed price
        # Item 3: New
        tee_times = [
            {"golf": "ClubA", "date": target_date, "time": "08:00", "hour_num": 8, "price": 10000}, # Same
            {"golf": "ClubB", "date": target_date, "time": "09:00", "hour_num": 9, "price": 20000}, # Changed (Old was 15000)
            {"golf": "ClubC", "date": target_date, "time": "10:00", "hour_num": 10, "price": 30000}, # New
        ]
        
        # 2. Existing Data (Firestore)
        # ClubA: Same
        # ClubB: Old Price
        # ClubD: To be deleted
        doc_a = MagicMock()
        doc_a.id = f"{target_date.replace('-', '')}_ClubA_0800"
        doc_a.to_dict.return_value = {"club_name": "ClubA", "date": target_date, "time": "08:00", "price": 10000}
        
        doc_b = MagicMock()
        doc_b.id = f"{target_date.replace('-', '')}_ClubB_0900"
        doc_b.to_dict.return_value = {"club_name": "ClubB", "date": target_date, "time": "09:00", "price": 15000} # Diff price
        
        doc_d = MagicMock()
        doc_d.id = f"{target_date.replace('-', '')}_ClubD_1100"
        doc_d.to_dict.return_value = {"club_name": "ClubD", "date": target_date, "time": "11:00", "price": 40000}
        
        self.mock_collection.where.return_value.stream.return_value = [doc_a, doc_b, doc_d]
        
        # Run
        save_tee_times(self.mock_db, tee_times, target_date)
        
        # Verify
        # 1. Delete: ClubD should be deleted
        self.mock_batch.delete.assert_called()
        deleted_refs = [call[0][0] for call in self.mock_batch.delete.call_args_list]
        # We can't easily check the ref path string without more mocking, but we can check count
        self.assertEqual(self.mock_batch.delete.call_count, 1, "Should delete 1 item (ClubD)")
        
        # 2. Upsert: ClubB (Changed) and ClubC (New) should be set. ClubA (Same) should be skipped.
        self.assertEqual(self.mock_batch.set.call_count, 2, "Should upsert 2 items (ClubB, ClubC)")
        
        # Verify ClubA was NOT updated
        # We check the args passed to set
        set_calls = self.mock_batch.set.call_args_list
        upserted_clubs = [call[0][1]['club_name'] for call in set_calls]
        self.assertIn("ClubB", upserted_clubs)
        self.assertIn("ClubC", upserted_clubs)
        self.assertNotIn("ClubA", upserted_clubs)
        
        print("save_tee_times optimization verified!")

    @patch('app.db')
    def test_get_prices_batching(self, mock_db_app):
        print("\nTesting get_prices batching...")
        
        # Setup
        mock_collection = MagicMock()
        mock_db_app.collection.return_value = mock_collection
        
        # Mock Request Data
        with app.test_request_context(json={
            "dates": ["2025-12-25"],
            "clubs": ["ClubA"],
            "times": []
        }):
            # Mock History Query (daily_stats)
            # Should be called with date = 2025-12-18 (7 days ago)
            hist_doc = MagicMock()
            hist_doc.to_dict.return_value = {"club_name": "ClubA", "hour": 8, "min_price": 5000}
            
            # Mock Current Data Query (tee_times)
            curr_doc = MagicMock()
            curr_doc.to_dict.return_value = {
                "club_name": "ClubA", 
                "date": "2025-12-25", 
                "time": "08:00", 
                "hour": 8, 
                "price": 10000,
                "source": "Test"
            }
            
            # Configure side_effect for stream() to return different iterators based on call
            # First call: History (daily_stats)
            # Second call: Current (tee_times)
            # But 'where' is called on collection.
            # We need to differentiate by collection name or arguments.
            
            def stream_side_effect(*args, **kwargs):
                # This is hard to mock perfectly because chaining: db.collection().where().stream()
                # We can check the collection name called.
                return []

            # Simpler approach: Verify 'where' calls
            # We expect:
            # 1. collection('daily_stats').where('date', '==', '2025-12-18')
            # 2. collection('tee_times').where('date', '==', '2025-12-25')
            
            # Let's mock the return values of where().stream()
            # We can't easily distinguish the two 'where' calls if they return the same mock object.
            # So we make collection() return different mocks based on name.
            
            mock_daily_stats = MagicMock()
            mock_tee_times = MagicMock()
            
            def collection_side_effect(name):
                if name == 'daily_stats': return mock_daily_stats
                if name == 'tee_times': return mock_tee_times
                return MagicMock()
                
            mock_db_app.collection.side_effect = collection_side_effect
            
            mock_daily_stats.where.return_value.stream.return_value = [hist_doc]
            mock_tee_times.where.return_value.stream.return_value = [curr_doc]
            
            # Run
            from app import get_prices
            response = get_prices()
            json_data = response.get_json()
            
            # Verify Results
            self.assertEqual(len(json_data), 1)
            item = json_data[0]
            self.assertEqual(item['club_name'], "ClubA")
            self.assertEqual(item['price'], 10000)
            self.assertEqual(item['history_price'], 5000) # Should come from history map
            self.assertEqual(item['diff'], 5000)
            
            # Verify Calls
            # Check if daily_stats was queried with correct date
            mock_daily_stats.where.assert_called_with('date', '==', '2025-12-18')
            
            print("get_prices batching verified!")

if __name__ == '__main__':
    unittest.main()
