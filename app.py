from flask import Flask
from flask import request, jsonify
import json
import os
import pandas as pd


app = Flask(__name__)
app.json.sort_keys = False


dir_ = "processed_data"  
files = [f for f in os.listdir(dir_) if f.endswith('.json')]
dataframes = []
for file in files:
    with open(os.path.join(dir_, file), 'r') as f:
        data = json.load(f)
        tmp_df = pd.json_normalize(data)  
        dataframes.append(tmp_df)

df = pd.concat(dataframes, ignore_index=True)

df["url"] = df["url"].str.strip().str.lower()
df["term"] = df["term"].str.strip()

print(df)

@app.route('/results', methods=['POST'])
def get_results():
    input_json = request.get_json()
    term = input_json.get("term")
    filtered_df = df.loc[df['term'] == term, ["url", "clicks"]]
    sorted_df = filtered_df.sort_values(by=["clicks", "url"], ascending=[False, True])
    sorted_df['domain_type'] = sorted_df['url'].apply(lambda x: x.split('.')[-1])
    sorted_df = sorted_df.sort_values(by=["clicks", "domain_type"], ascending=[False, True])
    sorted_df = sorted_df.drop(columns=['domain_type'])
    result_dict = sorted_df.set_index('url')['clicks'].to_dict()
    return jsonify({"results": result_dict})


@app.route('/trends', methods=['POST'])
def get_trends():
    input_json = request.get_json()
    term = input_json.get("term")
    total_count = df.loc[df['term'] == term, "clicks"].sum()
    return jsonify({"clicks": int(total_count)})


@app.route('/popularity', methods=['POST'])
def get_popularity():
    input_json = request.get_json()
    url = input_json.get("url")
    total_count = df.loc[df['url'] == url, "clicks"].sum()
    return jsonify({"clicks": int(total_count)})


@app.route("/getBestTerms", methods=["POST"])
def get_best_terms():
    data = request.get_json()
    website = data.get("website")
    df_site = df[df["url"] == website]
    total_clicks = df_site["clicks"].sum()
    best = df_site[df_site["clicks"] > 0.05 * total_clicks]
    best_terms = sorted(best["term"].unique().tolist())
    return jsonify({"best_terms": best_terms})


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8080, debug=True)
