import csv
from datetime import datetime

def convert_pos_transactions(input_csv, output_csv):
    """
    Converts the provided raw POS transactions (e.g. Brigade_Bangalore_10_April_26)
    into the format expected by our Store Intelligence pipeline:
    store_id, transaction_id, timestamp, basket_value_inr
    """
    with open(input_csv, 'r', encoding='utf-8') as infile, open(output_csv, 'w', encoding='utf-8', newline='') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.writer(outfile)
        
        # Write our pipeline's expected header
        writer.writerow(["store_id", "transaction_id", "timestamp", "basket_value_inr"])
        
        for row in reader:
            # Safely extract fields
            store_id = row.get("store_id", "STORE_UNKNOWN")
            tx_id = row.get("invoice_number", "")
            date_str = row.get("order_date", "")
            time_str = row.get("order_time", "")
            amount = row.get("total_amount", "0")
            
            try:
                # Assuming order_date is DD-MM-YYYY and time is HH:MM:SS
                dt_obj = datetime.strptime(f"{date_str} {time_str}", "%d-%m-%Y %H:%M:%S")
                timestamp_iso = dt_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                # Fallback if time format is unexpected
                timestamp_iso = f"{date_str}T{time_str}Z"
                
            writer.writerow([store_id, tx_id, timestamp_iso, amount])

if __name__ == "__main__":
    convert_pos_transactions(
        input_csv="Brigade_Bangalore_10_April_26 (1)bc6219c.csv", 
        output_csv="pos_transactions.csv"
    )
    print("Successfully converted POS transactions to pipeline format (pos_transactions.csv)")
