from flask import Flask, render_template, request, jsonify, send_file
import pandas as pd
import io

app = Flask(__name__)

# Global storage
dataset1 = None
dataset2 = None
joined_data = None
kpi_data = None
chart_data = None


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/upload_datasets", methods=["POST"])
def upload_datasets():
    global dataset1, dataset2

    file1 = request.files.get("file1")
    file2 = request.files.get("file2")

    if not file1 or not file2:
        return jsonify({"error": "Both files are required"}), 400

    def read_dataset(file):
        if file.filename.endswith(".csv"):
            return pd.read_csv(file)
        elif file.filename.endswith(".xlsx"):
            return pd.read_excel(file)
        else:
            raise ValueError("Unsupported file type")

    dataset1 = read_dataset(file1)
    dataset2 = read_dataset(file2)

    common_cols = list(set(dataset1.columns).intersection(set(dataset2.columns)))
    return jsonify({
        "dataset1_cols": list(dataset1.columns),
        "dataset2_cols": list(dataset2.columns),
        "common_columns": common_cols
    })


@app.route("/process_join", methods=["POST"])
def process_join():
    global dataset1, dataset2, joined_data, kpi_data

    join_columns = request.form.getlist("join_columns[]")
    join_type = request.form.get("join_type", "inner")

    if not join_columns:
        return jsonify({"error": "No join columns provided"}), 400

    joined_data = pd.merge(dataset1, dataset2, on=join_columns, how=join_type)
    kpi_data = None
    preview_data = joined_data.head(10).to_dict(orient="records")
    return jsonify({"joined_data": preview_data})


@app.route("/analyze_kpi", methods=["POST"])
def analyze_kpi():
    global dataset1, dataset2, joined_data, kpi_data, chart_data

    selected_dataset = request.form.get("dataset")
    if selected_dataset == "dataset1":
        df = dataset1
    elif selected_dataset == "dataset2":
        df = dataset2
    elif selected_dataset == "joined" and joined_data is not None:
        df = joined_data
    else:
        return jsonify({"error": "Dataset not available"}), 400

    kpis, tips = [], []
    chart_data = None

    if {"Units_Sold", "Price", "Discount"}.issubset(df.columns):
        df["Revenue"] = df["Units_Sold"] * df["Price"] * (1 - df["Discount"]/100)

    if "Revenue" in df.columns:
        total_revenue = df["Revenue"].sum()
        avg_discount = df["Discount"].mean() if "Discount" in df.columns else 0
        total_units = df["Units_Sold"].sum() if "Units_Sold" in df.columns else 0
        avg_rating = df["Rating"].mean() if "Rating" in df.columns else None

        kpis.extend([
            {"name": "Total Revenue (₹)", "value": round(total_revenue, 2)},
            {"name": "Total Units Sold", "value": int(total_units)},
            {"name": "Average Discount (%)", "value": round(avg_discount, 2)},
        ])
        if avg_rating:
            kpis.append({"name": "Average Rating", "value": round(avg_rating, 2)})

        if "Brand" in df.columns:
            top_brand = df.groupby("Brand")["Revenue"].sum().idxmax()
            kpis.append({"name": "Top Brand by Revenue", "value": top_brand})

        if "Region" in df.columns:
            top_region = df.groupby("Region")["Revenue"].sum().idxmax()
            kpis.append({"name": "Top Region by Revenue", "value": top_region})

    # Improvement Tips
    if "Discount" in df.columns and df["Discount"].mean() > 10:
        tips.append("💸 High average discounts — consider optimizing pricing strategy.")
    if "Units_Sold" in df.columns and df["Units_Sold"].sum() < 200:
        tips.append("🛒 Low total sales — increase marketing in underperforming regions.")
    if "Rating" in df.columns and df["Rating"].mean() < 4.3:
        tips.append("⭐ Customer satisfaction below target — improve service quality.")

    # Trend detection if Date exists
    if "Date" in df.columns and "Units_Sold" in df.columns:
        try:
            df["Date"] = pd.to_datetime(df["Date"])
            monthly = df.groupby(df["Date"].dt.to_period("M"))["Units_Sold"].sum().reset_index()
            monthly["Month"] = monthly["Date"].astype(str)
            monthly["Change"] = monthly["Units_Sold"].diff()
            trend = monthly["Change"].mean()
            if trend > 0:
                tips.append(f"📈 Sales trend increasing by {round(trend, 1)} units/month — maintain stock levels.")
            elif trend < 0:
                tips.append(f"📉 Sales trend decreasing by {abs(round(trend, 1))} units/month — investigate causes.")
            chart_data = monthly.rename(columns={"Units_Sold": "Count"})[["Month", "Count"]]
        except Exception:
            pass

    kpi_data = pd.DataFrame(kpis)
    chart_json = [] if chart_data is None else chart_data.to_dict(orient="records")

    return jsonify({
        "kpis": kpis,
        "chart_data": chart_json,
        "category": "Month" if chart_data is not None else None,
        "tips": tips
    })


@app.route("/export")
def export_excel():
    global kpi_data, chart_data

    if kpi_data is None:
        return jsonify({"error": "No KPI data to export"}), 400

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        kpi_data.to_excel(writer, index=False, sheet_name="KPI_Results")
        if chart_data is not None:
            chart_data.to_excel(writer, index=False, sheet_name="Chart_Data")
    output.seek(0)

    return send_file(output, as_attachment=True, download_name="kpi_analysis.xlsx")


if __name__ == "__main__":
    app.run(debug=True)
