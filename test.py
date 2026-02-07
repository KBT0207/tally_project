
import xml.etree.ElementTree as ET
import pandas as pd


# ==========================================
# STEP 2: Load XML and Find Vouchers
# ==========================================

def step1_load_xml(XML_PATH):
    # Parse XML file
    tree = ET.parse(XML_PATH)
    root = tree.getroot()
    
    # Find all vouchers
    vouchers = root.findall(".//VOUCHER")
    
    print("=" * 60)
    print("STEP 1: LOAD XML")
    print("=" * 60)
    print(f"Found {len(vouchers)} vouchers")
    print(f"\nFirst voucher tags: {[child.tag for child in list(vouchers[0])[:10]]}")
    
    return vouchers


# ==========================================
# STEP 3: Extract Basic Voucher Info (Non-List Fields)
# ==========================================

def step2_extract_basic_info(vouchers):
    """
    STEP 2: Extract simple fields (not lists)
    These are fields that appear once per voucher
    """
    
    print("\n" + "=" * 60)
    print("STEP 2: EXTRACT BASIC VOUCHER INFO")
    print("=" * 60)
    
    voucher = vouchers[1]  # Use second voucher as example
    
    # Extract basic fields
    date = voucher.find('DATE').text if voucher.find('DATE') is not None else ""
    voucher_no = voucher.find('VOUCHERNUMBER').text if voucher.find('VOUCHERNUMBER') is not None else ""
    party_name = voucher.find('PARTYNAME').text if voucher.find('PARTYNAME') is not None else ""
    narration = voucher.find('NARRATION').text if voucher.find('NARRATION') is not None else ""
    
    print(f"Date: {date}")
    print(f"Voucher Number: {voucher_no}")
    print(f"Party Name: {party_name}")
    print(f"Narration: {narration[:50]}...")
    
    return {
        'DATE': date,
        'VOUCHERNUMBER': voucher_no,
        'PARTYNAME': party_name,
        'NARRATION': narration
    }


# ==========================================
# STEP 4: Extract LEDGERENTRIES.LIST (First List)
# ==========================================

def step3_extract_ledger_entries(vouchers):
    """
    STEP 3: Extract LEDGERENTRIES.LIST
    This is a list that can have multiple items
    """
    
    print("\n" + "=" * 60)
    print("STEP 3: EXTRACT LEDGER ENTRIES (LIST)")
    print("=" * 60)
    
    voucher = vouchers[1]  # Use second voucher
    
    # Method 1: Find all LEDGERENTRIES.LIST
    ledger_entries = voucher.findall('.//LEDGERENTRIES.LIST')
    
    print(f"Found {len(ledger_entries)} ledger entries\n")
    
    # Loop through each ledger entry
    ledger_data = []
    for idx, ledger in enumerate(ledger_entries, 1):
        ledger_name = ledger.find('LEDGERNAME').text if ledger.find('LEDGERNAME') is not None else ""
        amount = ledger.find('AMOUNT').text if ledger.find('AMOUNT') is not None else ""
        
        print(f"Ledger {idx}:")
        print(f"  Name: {ledger_name}")
        print(f"  Amount: {amount}")
        
        ledger_data.append({
            'LEDGERNAME': ledger_name,
            'AMOUNT': amount
        })
    
    return ledger_data


# ==========================================
# STEP 5: Extract ALLINVENTORYENTRIES.LIST (Second List)
# ==========================================

def step4_extract_inventory_entries(vouchers):
    """
    STEP 4: Extract ALLINVENTORYENTRIES.LIST
    This is another list with item details
    """
    
    print("\n" + "=" * 60)
    print("STEP 4: EXTRACT INVENTORY ENTRIES (LIST)")
    print("=" * 60)
    
    voucher = vouchers[1]  # Use second voucher
    
    # Find all inventory entries
    inventory_entries = voucher.findall('.//ALLINVENTORYENTRIES.LIST')
    
    print(f"Found {len(inventory_entries)} inventory items\n")
    
    # Loop through each item
    inventory_data = []
    for idx, item in enumerate(inventory_entries, 1):
        stock_item = item.find('STOCKITEMNAME').text if item.find('STOCKITEMNAME') is not None else ""
        rate = item.find('RATE').text if item.find('RATE') is not None else ""
        qty = item.find('ACTUALQTY').text if item.find('ACTUALQTY') is not None else ""
        amount = item.find('AMOUNT').text if item.find('AMOUNT') is not None else ""
        
        print(f"Item {idx}:")
        print(f"  Stock Item: {stock_item}")
        print(f"  Rate: {rate}")
        print(f"  Qty: {qty}")
        print(f"  Amount: {amount}")
        
        inventory_data.append({
            'STOCKITEMNAME': stock_item,
            'RATE': rate,
            'ACTUALQTY': qty,
            'AMOUNT': amount
        })
    
    return inventory_data


# ==========================================
# STEP 6: Extract NESTED List (BATCHALLOCATIONS inside INVENTORY)
# ==========================================

def step5_extract_nested_batch(vouchers):
    """
    STEP 5: Extract BATCHALLOCATIONS.LIST (Nested inside ALLINVENTORYENTRIES)
    This is a list INSIDE another list
    """
    
    print("\n" + "=" * 60)
    print("STEP 5: EXTRACT NESTED BATCH ALLOCATIONS")
    print("=" * 60)
    
    voucher = vouchers[1]  # Use second voucher
    
    # Find all inventory entries
    inventory_entries = voucher.findall('.//ALLINVENTORYENTRIES.LIST')
    
    # Loop through each item
    all_data = []
    for idx, item in enumerate(inventory_entries, 1):
        stock_item = item.find('STOCKITEMNAME').text if item.find('STOCKITEMNAME') is not None else ""
        
        # Now find BATCHALLOCATIONS inside this item
        batch_entry = item.find('.//BATCHALLOCATIONS.LIST')
        
        if batch_entry is not None:
            batch_name = batch_entry.find('BATCHNAME').text if batch_entry.find('BATCHNAME') is not None else ""
            godown = batch_entry.find('GODOWNNAME').text if batch_entry.find('GODOWNNAME') is not None else ""
            
            print(f"Item {idx}: {stock_item}")
            print(f"  Batch: {batch_name}")
            print(f"  Godown: {godown}")
            
            all_data.append({
                'STOCKITEMNAME': stock_item,
                'BATCHNAME': batch_name,
                'GODOWNNAME': godown
            })
        else:
            print(f"Item {idx}: {stock_item} - No batch info")
    
    return all_data


# ==========================================
# STEP 7: Combine Everything into Rows
# ==========================================

def step6_combine_all_data(vouchers):
    """
    STEP 6: Combine voucher info + items into rows
    Each item becomes a separate row with voucher info repeated
    """
    
    print("\n" + "=" * 60)
    print("STEP 6: COMBINE INTO ROWS")
    print("=" * 60)
    
    all_rows = []
    
    # Process first 2 vouchers as example
    for v_idx, voucher in enumerate(vouchers[:2], 1):
        
        # Extract voucher basic info (will repeat for each item)
        voucher_no = voucher.find('VOUCHERNUMBER').text if voucher.find('VOUCHERNUMBER') is not None else ""
        party_name = voucher.find('PARTYNAME').text if voucher.find('PARTYNAME') is not None else ""
        
        # Extract inventory items
        inventory_entries = voucher.findall('.//ALLINVENTORYENTRIES.LIST')
        
        print(f"\nVoucher {v_idx}: {voucher_no}")
        print(f"  Party: {party_name}")
        print(f"  Items: {len(inventory_entries)}")
        
        # Create one row per item
        for item in inventory_entries:
            stock_item = item.find('STOCKITEMNAME').text if item.find('STOCKITEMNAME') is not None else ""
            rate = item.find('RATE').text if item.find('RATE') is not None else ""
            qty = item.find('ACTUALQTY').text if item.find('ACTUALQTY') is not None else ""
            amount = item.find('AMOUNT').text if item.find('AMOUNT') is not None else ""
            
            # Get batch info if exists
            batch_entry = item.find('.//BATCHALLOCATIONS.LIST')
            batch_name = ""
            godown = ""
            if batch_entry is not None:
                batch_name = batch_entry.find('BATCHNAME').text if batch_entry.find('BATCHNAME') is not None else ""
                godown = batch_entry.find('GODOWNNAME').text if batch_entry.find('GODOWNNAME') is not None else ""
            
            # Create row with voucher info + item info
            row = {
                'voucher_no': voucher_no,
                'party_name': party_name,
                'item': stock_item,
                'qty': qty,
                'rate': rate,
                'amount': amount,
                'batch': batch_name,
                'godown': godown
            }
            
            all_rows.append(row)
    
    # Convert to DataFrame
    df = pd.DataFrame(all_rows)
    
    print(f"\n📊 Created DataFrame with {len(df)} rows")
    print("\nSample Data:")
    print(df.to_string(index=False))
    
    return df


# ==========================================
# STEP 8: Complete Function - Process ALL Vouchers
# ==========================================

def step7_complete_extraction(xml_path):
    """
    STEP 7: Complete function to process all vouchers
    """
    
    print("\n" + "=" * 60)
    print("STEP 7: COMPLETE EXTRACTION - ALL VOUCHERS")
    print("=" * 60)
    
    # Parse XML
    tree = ET.parse(xml_path)
    root = tree.getroot()
    vouchers = root.findall(".//VOUCHER")
    
    all_rows = []
    
    # Process each voucher
    for voucher in vouchers:
        
        # Extract voucher info (repeats for each item)
        voucher_data = {
            'date': voucher.find('DATE').text if voucher.find('DATE') is not None else "",
            'voucher_no': voucher.find('VOUCHERNUMBER').text if voucher.find('VOUCHERNUMBER') is not None else "",
            'party_name': voucher.find('PARTYNAME').text if voucher.find('PARTYNAME') is not None else "",
            'narration': voucher.find('NARRATION').text if voucher.find('NARRATION') is not None else "",
        }
        
        # Extract items
        inventory_entries = voucher.findall('.//ALLINVENTORYENTRIES.LIST')
        
        if len(inventory_entries) == 0:
            # No items - create one row with blank item
            row = voucher_data.copy()
            row.update({
                'item': 'No Item',
                'qty': '',
                'rate': '',
                'amount': '',
                'batch': '',
                'godown': ''
            })
            all_rows.append(row)
        else:
            # Create one row per item
            for item in inventory_entries:
                row = voucher_data.copy()
                
                # Item details
                row['item'] = item.find('STOCKITEMNAME').text if item.find('STOCKITEMNAME') is not None else ""
                row['qty'] = item.find('ACTUALQTY').text if item.find('ACTUALQTY') is not None else ""
                row['rate'] = item.find('RATE').text if item.find('RATE') is not None else ""
                row['amount'] = item.find('AMOUNT').text if item.find('AMOUNT') is not None else ""
                
                # Batch info (nested)
                batch_entry = item.find('.//BATCHALLOCATIONS.LIST')
                if batch_entry is not None:
                    row['batch'] = batch_entry.find('BATCHNAME').text if batch_entry.find('BATCHNAME') is not None else ""
                    row['godown'] = batch_entry.find('GODOWNNAME').text if batch_entry.find('GODOWNNAME') is not None else ""
                else:
                    row['batch'] = ""
                    row['godown'] = ""
                
                all_rows.append(row)
    
    # Create DataFrame
    df = pd.DataFrame(all_rows)
    
    print(f"\n✅ Processed {len(vouchers)} vouchers")
    print(f"✅ Created {len(df)} rows")
    print(f"\nColumns: {list(df.columns)}")
    print(f"\nFirst 5 rows:")
    print(df.head().to_string(index=False))
    
    return df


# ==========================================
# RUN ALL STEPS
# ==========================================

def main():
    XML_PATH = "debug_sales_response_20260207_105311.xml"
    
    print("\n" + "=" * 60)
    print("XML FLATTENING TUTORIAL")
    print("=" * 60)
    
    # Step 1: Load XML
    vouchers = step1_load_xml(XML_PATH)
    
    # Step 2: Extract basic info
    basic_info = step2_extract_basic_info(vouchers)
    
    # Step 3: Extract ledger entries
    ledger_data = step3_extract_ledger_entries(vouchers)
    
    # Step 4: Extract inventory entries
    inventory_data = step4_extract_inventory_entries(vouchers)
    
    # Step 5: Extract nested batch allocations
    batch_data = step5_extract_nested_batch(vouchers)
    
    # Step 6: Combine into rows
    df_combined = step6_combine_all_data(vouchers)
    
    # Step 7: Complete extraction
    df_complete = step7_complete_extraction(XML_PATH)

    
    print("\n" + "=" * 60)
    print("TUTORIAL COMPLETE!")
    print("=" * 60)
    print("\nKey Learnings:")
    print("1. Use .find() for single elements")
    print("2. Use .findall() for lists (multiple elements)")
    print("3. Use .find('.//TAG') to search nested elements")
    print("4. Each list item becomes a separate row")
    print("5. Voucher info repeats for each item row")
    
    return df_complete


if __name__ == "__main__":
    df = main()
    df.to_excel('sample.xlsx',index=False)