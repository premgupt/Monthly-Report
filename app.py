from flask import Flask, request, send_file
import pandas as pd
from io import BytesIO

app = Flask(__name__)

@app.route("/run_monthly_report", methods=["POST"])
def run_monthly_report():
    bulk_file = request.files["bulk_file"]
    business_file = request.files["business_file"]

    # ========= Your Full Business Logic Starts Here =========

    # Load the uploaded files
    bulk_data = pd.read_excel(bulk_file, sheet_name=None)
    business_data = pd.read_csv(business_file)

    # Shortcuts for bulksheet tabs
    sp = bulk_data["Sponsored Products Campaigns"]
    sb = bulk_data["Sponsored Brands Campaigns"]
    sb_multi = bulk_data["SB Multi Ad Group Campaigns"]
    sd = bulk_data["Sponsored Display Campaigns"]

    # Rename Business Report columns
    business = business_data.rename(columns={
        "(Child) ASIN": "ASIN",
        "Sessions - Mobile App": "Sessions_Mobile_App",
        "Sessions - Mobile APP - B2B": "Sessions_Mobile_B2B",
        "Sessions - Browser": "Sessions_Browser",
        "Sessions - Browser - B2B": "Sessions_Browser_B2B",
        "Page Views - Mobile App": "PageViews_Mobile_App",
        "Page Views - Mobile APP - B2B": "PageViews_Mobile_B2B",
        "Page Views - Browser": "PageViews_Browser",
        "Page Views - Browser - B2B": "PageViews_Browser_B2B",
        "Sessions - Total": "Sessions_Total",
        "Sessions - Total - B2B": "Sessions_Total_B2B",
        "Ordered Product Sales": "Ordered_Sales",
        "Ordered Product Sales - B2B": "Ordered_Sales_B2B",
        "Total Order Items": "Orders",
        "Total Order Items - B2B": "Orders_B2B",
        "Page Views - Total": "PageViews_Total",
        "Page Views - Total - B2B": "PageViews_Total_B2B"
    })

    # ========== 1. Ad Type Level Performance ==========
    frames = []
    for df, source, video_col in [(sp, "SP", None), (sd, "SD", None), (sb, "SB", "Video Media IDs"), (sb_multi, "SB", "Video Asset IDs")]:
        df = df[df["Entity"] == "Campaign"].copy()
        if video_col:
            df["Ad Type"] = df[video_col].apply(lambda x: "SBV" if pd.notna(x) and str(x).strip() != "" else "SB")
        else:
            df["Ad Type"] = source
        frames.append(df)

    ad_type_data = pd.concat(frames)
    ad_type_perf = ad_type_data.groupby("Ad Type").agg({
        "Impressions": "sum", "Clicks": "sum", "Spend": "sum", "Sales": "sum", "Orders": "sum"
    }).reset_index()

    ad_type_perf["Click-through Rate"] = (ad_type_perf["Clicks"] / ad_type_perf["Impressions"]) * 100
    ad_type_perf["Conversion Rate"] = (ad_type_perf["Orders"] / ad_type_perf["Clicks"]) * 100
    ad_type_perf["ACOS"] = (ad_type_perf["Spend"] / ad_type_perf["Sales"]) * 100
    ad_type_perf["ROAS"] = (ad_type_perf["Sales"] / ad_type_perf["Spend"])
    ad_type_perf["CPC"] = (ad_type_perf["Spend"] / ad_type_perf["Clicks"])
    ad_type_perf["Spend Share (%)"] = (ad_type_perf["Spend"] / ad_type_perf["Spend"].sum()) * 100
    ad_type_perf = ad_type_perf.round(2)

    # ========== 2. Match Type Performance ==========
    match_frames = []
    for df, match_col in [(sp, "Match Type"), (sb, "Match Type"), (sb_multi, "Match Type")]:
        temp = df[df["Entity"] == "Keyword"].copy()
        temp = temp.groupby(match_col).agg({
            "Impressions": "sum", "Clicks": "sum", "Spend": "sum", "Sales": "sum", "Orders": "sum"
        }).reset_index().rename(columns={match_col: "Match Type"})
        match_frames.append(temp)

    match_perf = pd.concat(match_frames).groupby("Match Type").sum(min_count=1).reset_index()
    match_perf["Click-through Rate"] = (match_perf["Clicks"] / match_perf["Impressions"]) * 100
    match_perf["Conversion Rate"] = (match_perf["Orders"] / match_perf["Clicks"]) * 100
    match_perf["ACOS"] = (match_perf["Spend"] / match_perf["Sales"]) * 100
    match_perf["ROAS"] = (match_perf["Sales"] / match_perf["Spend"])
    match_perf["CPC"] = (match_perf["Spend"] / match_perf["Clicks"])
    match_perf["Spend Share (%)"] = (match_perf["Spend"] / match_perf["Spend"].sum()) * 100
    match_perf = match_perf.round(2)

    # ========== 3. Bidding Strategy Performance ==========
    bid_perf = sp[sp["Entity"] == "Campaign"].copy()
    bid_perf = bid_perf.groupby("Bidding Strategy").agg({
        "Impressions": "sum", "Clicks": "sum", "Spend": "sum", "Sales": "sum", "Orders": "sum"
    }).reset_index()

    bid_perf["Click-through Rate"] = (bid_perf["Clicks"] / bid_perf["Impressions"]) * 100
    bid_perf["Conversion Rate"] = (bid_perf["Orders"] / bid_perf["Clicks"]) * 100
    bid_perf["ACOS"] = (bid_perf["Spend"] / bid_perf["Sales"]) * 100
    bid_perf["ROAS"] = (bid_perf["Sales"] / bid_perf["Spend"])
    bid_perf["CPC"] = (bid_perf["Spend"] / bid_perf["Clicks"])
    bid_perf["Spend Share (%)"] = (bid_perf["Spend"] / bid_perf["Spend"].sum()) * 100
    bid_perf = bid_perf.round(2)

    # ========== 4. Placement-Level Performance ==========
    placement_perf = sp[sp["Entity"] == "Bidding Adjustment"].copy()
    placement_perf = placement_perf.groupby("Placement").agg({
        "Impressions": "sum", "Clicks": "sum", "Spend": "sum", "Sales": "sum", "Orders": "sum"
    }).reset_index()

    placement_perf["Click-through Rate"] = (placement_perf["Clicks"] / placement_perf["Impressions"]) * 100
    placement_perf["Conversion Rate"] = (placement_perf["Orders"] / placement_perf["Clicks"]) * 100
    placement_perf["ACOS"] = (placement_perf["Spend"] / placement_perf["Sales"]) * 100
    placement_perf["ROAS"] = (placement_perf["Sales"] / placement_perf["Spend"])
    placement_perf["CPC"] = (placement_perf["Spend"] / placement_perf["Clicks"])
    placement_perf["Spend Share (%)"] = (placement_perf["Spend"] / placement_perf["Spend"].sum()) * 100
    placement_perf = placement_perf.round(2)

    # ========== 5. Product-Level (PPC + Organic) ==========
    sp_prod = sp[sp["Entity"] == "Product Ad"].rename(columns={"ASIN (Informational only)": "ASIN"}).copy()
    sd_prod = sd[sd["Entity"] == "Product Ad"].rename(columns={"ASIN (Informational only)": "ASIN"}).copy()

    ppc_df = pd.concat([sp_prod, sd_prod])
    ppc_agg = ppc_df.groupby("ASIN").agg({"Spend": "sum", "Sales": "sum"}).reset_index()
    ppc_agg.columns = ["ASIN", "PPC Spend", "PPC Sales"]

    business["Overall Sales"] = business["Ordered_Sales"] + business["Ordered_Sales_B2B"]
    business["Overall Orders"] = business["Orders"] + business["Orders_B2B"]
    business["Sessions Combined"] = business["Sessions_Total"] + business["Sessions_Total_B2B"]
    business["Page Views Combined"] = business["PageViews_Total"] + business["PageViews_Total_B2B"]

    merged_prod = pd.merge(business, ppc_agg, on="ASIN", how="left").fillna(0)
    merged_prod["Organic Sales"] = merged_prod["Overall Sales"] - merged_prod["PPC Sales"]
    merged_prod["ACOS"] = (merged_prod["PPC Spend"] / merged_prod["PPC Sales"]) * 100
    merged_prod["TACOS"] = (merged_prod["PPC Spend"] / merged_prod["Overall Sales"]) * 100
    merged_prod["Overall CVR"] = (merged_prod["Overall Orders"] / merged_prod["Sessions Combined"]) * 100

    prod_perf = merged_prod[[
        "ASIN", "Overall Sales", "PPC Sales", "PPC Spend", "Organic Sales",
        "Overall Orders", "ACOS", "TACOS", "Sessions Combined", "Page Views Combined", "Overall CVR"
    ]]
    prod_perf = prod_perf.round(2)
    prod_perf["Sessions Combined"] = prod_perf["Sessions Combined"].astype(int)
    prod_perf["Page Views Combined"] = prod_perf["Page Views Combined"].astype(int)
    prod_perf["Overall Orders"] = prod_perf["Overall Orders"].astype(int)
    prod_perf = prod_perf.sort_values(by="Overall Sales", ascending=False)

    # ========== 6. Device Type ==========
    device_perf = pd.DataFrame({
        "Device Type": ["Mobile", "Web"],
        "Sessions": [
            (business["Sessions_Mobile_App"] + business["Sessions_Mobile_B2B"]).sum(),
            (business["Sessions_Browser"] + business["Sessions_Browser_B2B"]).sum()
        ],
        "Page Views": [
            (business["PageViews_Mobile_App"] + business["PageViews_Mobile_B2B"]).sum(),
            (business["PageViews_Browser"] + business["PageViews_Browser_B2B"]).sum()
        ]
    })
    device_perf["Page Views per Session"] = (device_perf["Page Views"] / device_perf["Sessions"]).round(2)
    device_perf["Sessions"] = device_perf["Sessions"].astype(int)
    device_perf["Page Views"] = device_perf["Page Views"].astype(int)

    # ========== SAVE TO EXCEL ==========
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        startrow = 0
        for df in [ad_type_perf, match_perf, bid_perf, placement_perf, prod_perf, device_perf]:
            df.to_excel(writer, index=False, sheet_name="Performance Summary", startrow=startrow)
            startrow += len(df) + 3
    output.seek(0)

    # ========= End of Business Logic =========

    return send_file(output, as_attachment=True, download_name="Performance_Summary.xlsx")

@app.route("/")
def home():
    return "Monthly Report API is live!"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
