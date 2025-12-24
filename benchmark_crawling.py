import time
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from crawler_utils import crawl_golfpang, crawl_teescan

# Mock DB save to isolate crawling performance
def mock_save(data, date):
    pass

def crawl_date(target_date):
    """Crawl a single date and return data count"""
    try:
        # Crawl Golfpang
        data_gp = crawl_golfpang(target_date, [])
        # Crawl Teescan
        data_ts = crawl_teescan(target_date, [])
        return data_gp + data_ts
    except Exception as e:
        print(f"Error on {target_date}: {e}")
        return []

def run_serial(dates):
    print("\n--- Starting Serial Crawl ---")
    start_time = time.time()
    total_items = 0
    results = {}
    
    for date in dates:
        print(f"Crawling {date}...")
        data = crawl_date(date)
        total_items += len(data)
        results[date] = len(data)
        
    duration = time.time() - start_time
    print(f"Serial finished in {duration:.2f}s. Total items: {total_items}")
    return results, duration

def run_parallel(dates):
    print("\n--- Starting Parallel Crawl (4 workers) ---")
    start_time = time.time()
    total_items = 0
    results = {}
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_date = {executor.submit(crawl_date, date): date for date in dates}
        
        for future in as_completed(future_to_date):
            date = future_to_date[future]
            try:
                data = future.result()
                total_items += len(data)
                results[date] = len(data)
                print(f"Finished {date} ({len(data)} items)")
            except Exception as e:
                print(f"Error on {date}: {e}")
                
    duration = time.time() - start_time
    print(f"Parallel finished in {duration:.2f}s. Total items: {total_items}")
    return results, duration

if __name__ == "__main__":
    # Test with just 3 days to be quick but representative
    today = datetime.date.today()
    dates = [(today + datetime.timedelta(days=i)).strftime("%Y-%m-%d") for i in range(3)]
    
    print(f"Benchmarking crawling for dates: {dates}")
    
    serial_counts, serial_time = run_serial(dates)
    parallel_counts, parallel_time = run_parallel(dates)
    
    print("\n\n=== Benchmark Results ===")
    print(f"Serial Time:   {serial_time:.2f}s")
    print(f"Parallel Time: {parallel_time:.2f}s")
    print(f"Speedup:       {serial_time / parallel_time:.2f}x")
    
    print("\n=== Data Integrity Check ===")
    match = True
    for date in dates:
        s_count = serial_counts.get(date, 0)
        p_count = parallel_counts.get(date, 0)
        if s_count != p_count:
            print(f"MISMATCH on {date}: Serial={s_count}, Parallel={p_count}")
            match = False
        else:
            print(f"Match on {date}: {s_count} items")
            
    if match:
        print("\nSUCCESS: Data counts match exactly!")
    else:
        print("\nWARNING: Data counts do not match!")
