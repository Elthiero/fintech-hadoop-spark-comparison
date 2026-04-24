import sys
import os

# Target columns based on the raw IEEE-CIS dataset
TARGET_COLS = {'card4', 'TransactionAmt', 'isFraud'}
col_indices = {}

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    columns = line.split(',')

    # 1. Catch the header to map column names to their exact index position
    if line.startswith('TransactionID'):
        for i, col_name in enumerate(columns):
            if col_name in TARGET_COLS:
                col_indices[col_name] = i
        continue

    # Safety check: ensure the header was parsed before processing data rows
    if not col_indices or len(columns) < 300: # Raw file has 394 columns
        continue

    try:
        # 2. Extract fields using the dynamic indices
        card_type = columns[col_indices['card4']].strip()
        
        # Replicate the Pandas fillna('unknown') logic for empty CSV fields (,,)
        if not card_type:
            card_type = 'unknown'

        amount = float(columns[col_indices['TransactionAmt']])
        is_fraud = int(columns[col_indices['isFraud']])

        # 3. Output to Reducer: card_type \t amount \t is_fraud \t 1
        print(f"{card_type}\t{amount}\t{is_fraud}\t1")

    except (IndexError, ValueError) as e:
        # Log unparseable rows to YARN logs and keep crunching
        sys.stderr.write(f"WARNING: Skipping line - {e}\n")
        continue