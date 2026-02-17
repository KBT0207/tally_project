import requests
import xml.etree.ElementTree as ET
import re
import pandas as pd
from datetime import datetime

# ==========================================
# 🔹 CONFIGURATION
# ==========================================

url = "http://localhost:9000"
headers = {"Content-Type": "text/xml"}
path = "utils/cdc/sales_cdc.xml"

# company_name = "KAY BEE BIO-ORGANICS PVT LTD (MH-PHALTAN) (From Apr-23)"
company_name = "Freshnova Pvt Ltd (FCY)"
alter_id = 0

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

raw_xml_file = f"tally_raw_{timestamp}.xml"
clean_xml_file = f"tally_clean_{timestamp}.xml"
excel_file = f"sales_full_export_{timestamp}.xlsx"

# ==========================================
# 🔹 LOAD XML TEMPLATE
# ==========================================

with open(path, "r", encoding="utf-8") as file:
    xml_template = file.read()

xml_data = xml_template.replace("{{COMPANY_NAME}}", company_name)
xml_data = xml_data.replace("{{ALTER_ID}}", str(alter_id))

# ==========================================
# 🔹 SEND REQUEST
# ==========================================

response = requests.post(
    url,
    data=xml_data.encode("utf-8"),
    headers=headers
)

if response.status_code != 200:
    print("❌ Error:", response.status_code)
    print(response.text)
    exit()

print("✅ Response received")

# ==========================================
# 🔹 SAVE RAW XML
# ==========================================

with open(raw_xml_file, "w", encoding="utf-8") as f:
    f.write(response.text)

print(f"📄 Raw XML saved: {raw_xml_file}")

clean_xml = response.text

# ==========================================
# 🔥 CLEAN XML
# ==========================================

clean_xml = re.sub(r"<UDF:.*?>", "", clean_xml)
clean_xml = re.sub(r"</UDF:.*?>", "", clean_xml)
clean_xml = re.sub(r"UDF:", "", clean_xml)
clean_xml = re.sub(r"&#\d+;", "", clean_xml)
clean_xml = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", clean_xml)

# ==========================================
# 🔹 SAVE CLEAN XML
# ==========================================

with open(clean_xml_file, "w", encoding="utf-8") as f:
    f.write(clean_xml)

print(f"🧹 Clean XML saved: {clean_xml_file}")

# ==========================================
# 🔹 FLATTEN FUNCTION
# ==========================================

def flatten_xml(element, parent_key="", result=None):
    if result is None:
        result = {}

    for child in element:
        key = f"{parent_key}_{child.tag}" if parent_key else child.tag

        if list(child):
            flatten_xml(child, key, result)
        else:
            value = child.text.strip() if child.text else ""
            result[key] = value

    return result

# ==========================================
# 🔹 PARSE XML
# ==========================================

root = ET.fromstring(clean_xml)
vouchers = root.findall(".//VOUCHER")

print("📦 Total vouchers:", len(vouchers))

data = []
max_alter_id = alter_id

for voucher in vouchers:
    flat_data = flatten_xml(voucher)
    flat_data["Company_Name"] = company_name

    alter_val = flat_data.get("ALTERID", "0")
    if alter_val.isdigit():
        max_alter_id = max(max_alter_id, int(alter_val))

    data.append(flat_data)

print("🚀 Latest AlterID:", max_alter_id)

# ==========================================
# 🔹 EXPORT TO EXCEL
# ==========================================

if data:
    df = pd.DataFrame(data)
    df.to_excel(excel_file, index=False)
    print(f"📁 Excel file created: {excel_file}")
else:
    print("⚠ No data found")
