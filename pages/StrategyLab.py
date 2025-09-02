import streamlit as st
import json, yaml, sqlite3

DB_PATH = r"C:\teevra18\data\teevra18.db"
with open(r"C:\teevra18\data\dhan_indicators.json") as f:
    INDICATORS = json.load(f)

st.title("🧪 Strategy Lab: All DhanHQ Indicators")

mode = st.radio("Choose Mode", ["Graphical", "Script"])

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

if mode == "Graphical":
    selected = st.multiselect("Select Indicators", list(INDICATORS.keys()))
    params = {}
    for ind in selected:
        st.subheader(ind)
        params[ind] = {}
        for p, default in INDICATORS[ind]["params"].items():
            params[ind][p] = st.number_input(f"{ind} - {p}", value=default)

    if st.button("Save Strategy"):
        cur.execute("INSERT INTO strategies_catalog(strategy_id,name,params_json,enabled,source_mode) VALUES (?,?,?,?,?)",
                    ("custom_lab","Custom Strategy",json.dumps(params),1,"graphical"))
        conn.commit()
        st.success("Saved!")

else:  # Script mode
    script_text = st.text_area("Paste YAML/JSON config")
    if st.button("Validate & Save"):
        try:
            cfg = yaml.safe_load(script_text)
            cur.execute("INSERT INTO strategies_catalog(strategy_id,name,params_json,enabled,source_mode) VALUES (?,?,?,?,?)",
                        ("custom_script","Script Strategy",json.dumps(cfg),1,"script"))
            conn.commit()
            st.success("Imported & Saved!")
        except Exception as e:
            st.error(f"Validation failed: {e}")

conn.close()
