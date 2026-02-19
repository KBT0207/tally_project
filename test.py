import requests
import xml.etree.ElementTree as ET
from datetime import datetime

COMPANY = "Freshnova Pvt Ltd (FCY)"
TALLY_URL = "http://localhost:9000"

def fetch_sales_vouchers(last_alter_id):

    print("Requesting vouchers with ALTERID >", last_alter_id)

    from_date = "20000101"
    to_date = datetime.now().strftime("%Y%m%d")

    xml_data = f"""<?xml version="1.0" encoding="UTF-8"?>
<ENVELOPE>
    <HEADER>
        <VERSION>1</VERSION>
        <TALLYREQUEST>Export</TALLYREQUEST>
        <TYPE>Data</TYPE>
        <ID>Voucher Register</ID>
    </HEADER>

    <BODY>
        <DESC>
            <STATICVARIABLES>
                <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
                <SVCURRENTCOMPANY>{COMPANY}</SVCURRENTCOMPANY>
                <SVFROMDATE>{from_date}</SVFROMDATE>
                <SVTODATE>{to_date}</SVTODATE>

                <!-- Sales Only -->
                <VoucherTypeName>Sales</VoucherTypeName>
            </STATICVARIABLES>

            <FETCHLIST>
                <FETCH>GUID</FETCH>
                <FETCH>ALTERID</FETCH>
                <FETCH>MASTERID</FETCH>
                <FETCH>VOUCHERNUMBER</FETCH>
                <FETCH>VOUCHERTYPENAME</FETCH>
                <FETCH>DATE</FETCH>
                <FETCH>PARTYNAME</FETCH>
                <FETCH>NARRATION</FETCH>
                <FETCH>ISDELETED</FETCH>
            </FETCHLIST>

            <FILTER>
                $$AlterID &gt; {last_alter_id}
            </FILTER>

        </DESC>
    </BODY>
</ENVELOPE>
"""

    try:
        response = requests.post(
            TALLY_URL,
            data=xml_data,
            headers={"Content-Type": "text/xml"},
            timeout=300
        )

        response.raise_for_status()

        if "<LINEERROR>" in response.text:
            print("Tally Error:")
            print(response.text)
            return None

        filename = f"sales_after_{last_alter_id}.xml"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(response.text)

        print("Saved:", filename)

        root = ET.fromstring(response.text)
        alter_ids = [
            int(node.text)
            for node in root.findall(".//ALTERID")
            if node.text and node.text.isdigit()
        ]

        if alter_ids:
            new_max = max(alter_ids)
            print("New Max ALTERID:", new_max)
            return new_max
        else:
            print("No new vouchers found.")
            return last_alter_id

    except Exception as e:
        print("Error:", e)
        return None


if __name__ == "__main__":
    last_id = 467
    last_id = fetch_sales_vouchers(last_id)