"""
DIAGNOSTIC SCRIPT: Test Currency Detection
============================================
This script will help you verify if the currency detection is working correctly.
"""

import sys
sys.path.insert(0, '')

from services.currency_extractor import CurrencyExtractor

# Test data from your actual vouchers
test_cases = [
    {
        'voucher': 'KBE24250001',
        'item': 'ALPHONSO MANGO (COUNT 12)',
        'rate': '3403.75 $ @ ? 82.92/ $ = ? 1612.79',  # Example USD format
        'amount': '3403.75 $ @ ? 82.92/ $ = ? 282238.95',
        'expected_currency': 'USD',
        'expected_foreign_amount': 3403.75,
        'expected_exchange_rate': 82.92,
        'expected_inr_amount': 282238.95
    },
    {
        'voucher': 'KBE/24-25/0001',
        'item': 'BABY CORN (60 Bags X 130 gms)',
        'rate': '33.93 £ = ? 3568.76/Box',  # Example GBP format
        'amount': '6243.12 £ @ ? 105.18/ £ = ? 656651.36',
        'expected_currency': 'GBP',
        'expected_foreign_amount': 6243.12,
        'expected_exchange_rate': 105.18,
        'expected_inr_amount': 656651.36
    },
    {
        'voucher': 'TEST-INR',
        'item': 'Test INR Item',
        'rate': '100',
        'amount': '1000',
        'expected_currency': 'INR',
        'expected_foreign_amount': None,
        'expected_exchange_rate': 1.0,
        'expected_inr_amount': 1000.0
    }
]

def extract_foreign_currency_info_test(rate_text=None, amount_text=None):
    """Test version of the extraction function"""
    extractor = CurrencyExtractor(default_currency='INR')
    
    result = {
        'currency': 'INR',
        'foreign_amount': None,
        'exchange_rate': 1.0,
        'inr_amount': None
    }
    
    # Detect currency from BOTH fields
    detected_currency = 'INR'
    
    # Check rate field
    if rate_text:
        rate_currency = extractor.extract_currency(rate_text)
        print(f"  Rate text: '{rate_text}'")
        print(f"  Detected from rate: {rate_currency}")
        if rate_currency and rate_currency != 'INR':
            detected_currency = rate_currency
    
    # Check amount field (most reliable)
    if amount_text:
        amount_currency = extractor.extract_currency(amount_text)
        print(f"  Amount text: '{amount_text}'")
        print(f"  Detected from amount: {amount_currency}")
        if amount_currency and amount_currency != 'INR':
            detected_currency = amount_currency
    
    result['currency'] = detected_currency
    print(f"  FINAL CURRENCY: {detected_currency}")
    
    # Extract details based on currency
    if detected_currency == 'INR':
        result['exchange_rate'] = 1.0
        result['foreign_amount'] = None
        if amount_text:
            import re
            match = re.search(r'([-]?\d+\.?\d*)', str(amount_text))
            if match:
                result['inr_amount'] = float(match.group(1))
    else:
        # Foreign currency
        if amount_text:
            amount_details = extractor.extract_foreign_currency_details(amount_text)
            if amount_details['foreign_amount']:
                result['foreign_amount'] = amount_details['foreign_amount']
            if amount_details['exchange_rate']:
                result['exchange_rate'] = amount_details['exchange_rate']
            if amount_details['base_amount']:
                result['inr_amount'] = amount_details['base_amount']
    
    return result

print("="*80)
print("CURRENCY DETECTION DIAGNOSTIC TEST")
print("="*80)

all_passed = True

for i, test in enumerate(test_cases, 1):
    print(f"\n{'='*80}")
    print(f"Test {i}: {test['voucher']} - {test['item']}")
    print(f"{'='*80}")
    
    result = extract_foreign_currency_info_test(test['rate'], test['amount'])
    
    print(f"\nResults:")
    print(f"  Currency: {result['currency']} (Expected: {test['expected_currency']})")
    print(f"  Foreign Amount: {result['foreign_amount']} (Expected: {test['expected_foreign_amount']})")
    print(f"  Exchange Rate: {result['exchange_rate']} (Expected: {test['expected_exchange_rate']})")
    print(f"  INR Amount: {result['inr_amount']} (Expected: {test['expected_inr_amount']})")
    
    # Check if passed
    passed = (
        result['currency'] == test['expected_currency'] and
        result['foreign_amount'] == test['expected_foreign_amount'] and
        result['exchange_rate'] == test['expected_exchange_rate']
    )
    
    if passed:
        print(f"\n✓ TEST PASSED")
    else:
        print(f"\n✗ TEST FAILED")
        all_passed = False

print(f"\n{'='*80}")
if all_passed:
    print("✓✓✓ ALL TESTS PASSED - Currency detection is working correctly!")
else:
    print("✗✗✗ SOME TESTS FAILED - Currency detection needs fixing")
print(f"{'='*80}")

print("\n" + "="*80)
print("NOW TEST WITH YOUR ACTUAL XML DATA")
print("="*80)
print("\nTo test with real data, you need to:")
print("1. Enable debug mode in tally_connector.py")
print("2. This will save the XML response to a file")
print("3. We can then extract the actual RATE and AMOUNT fields from the XML")
print("4. And test if currency detection works on them")
print("\nOr you can manually provide the RATE and AMOUNT text from the XML...")