from flask import Flask, request, send_file
import pandas as pd
from io import BytesIO

app = Flask(__name__)

@app.route("/run_monthly_report", methods=["POST"])
def run_monthly_report():
    bulk_file = request.files["bulk_file"]
    business_file = request.files["business_file"]

    # Load data
    bulk_data = pd.read_excel(bulk_file, sheet_name=None)
    business_data = pd.read_csv(business_file)

    # Shortcuts for bulksheet tabs
    sp = bulk_data["Sponsored Products Campaigns"]
    sb = bulk_data["Sponsored Brands Campaigns"]
    sb_multi = bulk_data["SB Multi Ad Group Campaigns"]
    sd = bulk_data["Sponsored Display Campaigns"]

    # Simple version: save top rows from SP
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        sp.head(10).to_excel(writer, sheet_name="Performance Summary", index=False)

    output.seek(0)
    return send_file(output, as_attachment=True, download_name="Performance_Summary.xlsx")

@app.route("/")
def home():
    return "Monthly Report API is live!"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
