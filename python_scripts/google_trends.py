from pytrends.request import TrendReq
import pandas as pd

def fetch_google_trends_90_days(keyword="BrainPOP", geo="US"):
    """
    Fetch Google Trends data for a keyword over the past 90 days.
    """
    try:
        pytrends = TrendReq(hl="en-US", tz=360)
        pytrends.build_payload([keyword], timeframe="today 3-m", geo=geo)
        trends = pytrends.interest_over_time()
        if not trends.empty:
            trends = trends.reset_index()[['date', keyword]] 
            trends.rename(columns={keyword: "frequency"}, inplace=True)
            return trends
        else:
            print(f"No data found for the keyword: {keyword}")
            return pd.DataFrame()
    except Exception as e:
        print(f"Error fetching Google Trends data: {e}")
        return pd.DataFrame()
