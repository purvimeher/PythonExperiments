import streamlit as st
import pandas as pd

from config import COLLECTIONS
from utils.helpers import get_collection_summary

st.title("📊 Dashboard")
st.markdown("Overview of all inventory collections")

summaries = [get_collection_summary(name) for name in COLLECTIONS.keys()]
df = pd.DataFrame(summaries)

c1, c2, c3 = st.columns(3)
with c1:
    st.metric("Collections", len(df))
with c2:
    st.metric("Total Records", int(df["records"].sum()) if not df.empty else 0)
with c3:
    st.metric("Total Qty", int(df["qty_sum"].sum()) if not df.empty else 0)

st.subheader("Collection Summary")
st.dataframe(
    df[["title", "records", "qty_sum"]].rename(
        columns={"title": "Collection", "records": "Records", "qty_sum": "Qty Total"}
    ),
    use_container_width=True,
    hide_index=True,
)