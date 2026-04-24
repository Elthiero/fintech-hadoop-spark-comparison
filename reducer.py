import sys

current_card = None
sum_amount = 0.0
total_count = 0
fraud_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")

    if len(parts) != 4:
        # LOUD FAILURE: Tell Hadoop exactly what the bad line looks like
        sys.stderr.write(f"WARNING: Skipping line, expected 4 parts but got {len(parts)}: '{line}'\n")
        continue

    card, amt_str, fraud_str, cnt_str = parts

    try:
        amt = float(amt_str)
        fraud = int(fraud_str)
        cnt = int(cnt_str)
    except ValueError as e:
        # LOUD FAILURE: Tell Hadoop why the math failed
        sys.stderr.write(f"WARNING: Cast error on line '{line}' - {e}\n")
        continue

    if current_card == card:
        sum_amount += amt
        total_count += cnt
        fraud_count += fraud
    else:
        if current_card is not None:
            avg_amount = sum_amount / total_count if total_count > 0 else 0.0
            fraud_pct = (fraud_count / total_count * 100) if total_count > 0 else 0.0
            print(f"{current_card}\t{sum_amount:.2f}\t{avg_amount:.2f}\t{fraud_pct:.2f}")
        current_card = card
        sum_amount = amt
        total_count = cnt
        fraud_count = fraud

if current_card is not None:
    avg_amount = sum_amount / total_count if total_count > 0 else 0.0
    fraud_pct = (fraud_count / total_count * 100) if total_count > 0 else 0.0
    print(f"{current_card}\t{sum_amount:.2f}\t{avg_amount:.2f}\t{fraud_pct:.2f}")