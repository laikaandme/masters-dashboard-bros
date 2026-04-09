# Masters leaderboard app

A Streamlit app for tracking your bros group's Masters picks and ranking everyone by the combined score of their five selected golfers.

### How to run it on your own machine

1. Install the requirements

   ```
   $ pip install -r requirements.txt
   ```

2. Run the app

   ```
   $ streamlit run streamlit_app.py
   ```

### CSV format

- Place a CSV file at `data/golf_picks.csv` or upload one from the sidebar.
- Required columns: one player column and five golfer pick columns.
- Optional columns: `Total Score` or individual score columns such as `Score 1`, `Score 2`, etc.
