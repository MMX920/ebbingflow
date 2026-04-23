import sys
import os
import math
from datetime import datetime, timedelta, timezone

# Ensure project root is in path
sys.path.append(os.getcwd())

from memory.scoring.hybrid_scorer import (
    TimeDecayCalculator, 
    HALF_LIFE_DAYS, 
    MIN_TIME_DECAY, 
    LOW_CONFIDENCE_THRESHOLD
)

def run_test():
    print("="*60)
    print("EbbingFlow Forgetting Curve Logic Test")
    print(f"Parameters: HALF_LIFE={HALF_LIFE_DAYS}, MIN_DECAY={MIN_TIME_DECAY}, THRESHOLD={LOW_CONFIDENCE_THRESHOLD}")
    print("="*60)
    
    calc = TimeDecayCalculator()
    now = datetime.now(timezone.utc)
    
    # 4 Test Scenarios
    test_cases = [
        ("A", 30, 0.9, "Close memory, high confidence (Should be > 0.25)"),
        ("B", 120, 0.95, "Old memory, high confidence (Should floor to 0.25)"),
        ("C", 120, 0.6, "Old memory, low confidence (Should be < 0.25, no floor)"),
        ("D", 3, 0.5, "Very fresh memory, low confidence (Should be raw decay, > 0.25)")
    ]
    
    print(f"{'Case':<5} | {'Days':<5} | {'Conf':<5} | {'Raw':<8} | {'Final':<8} | {'Status'}")
    print("-" * 65)
    
    for label, days, conf, desc in test_cases:
        # Calculate raw decay manually for comparison
        raw_decay = math.exp(-0.693 * days / HALF_LIFE_DAYS)
        
        # Calculate using our calculator
        ts = (now - timedelta(days=days)).isoformat()
        final_decay = calc.calculate(ts, confidence=conf)
        
        status = "PASS"
        if label == "A":
            if final_decay < 0.25: status = "FAIL"
        elif label == "B":
            if abs(final_decay - 0.25) > 1e-6: status = "FAIL"
        elif label == "C":
            if final_decay >= 0.25: status = "FAIL"
        elif label == "D":
            if final_decay < 0.25: status = "FAIL"
            
        print(f"{label:<5} | {days:<5} | {conf:<5} | {raw_decay:<8.4f} | {final_decay:<8.4f} | {status}")
        
    print("="*60)

if __name__ == "__main__":
    run_test()
