import pandas as pd

# Function to compute 30-day average volume
def compute_30_day_average(historical_file, target_dates):
    # Load historical daily data
    historical_data = pd.read_csv(historical_file, parse_dates=["Date"], dayfirst=True)

    # Calculate the 30-day average volume for each stock for the target dates
    average_volumes = {}
    for target_date in target_dates:
        # Filter the last 30 trading days prior to the target date
        date_filter = (historical_data["Date"] < target_date) & (historical_data["Date"] >= target_date - pd.Timedelta(days=30))
        stock_data = historical_data[date_filter]
        
        # Calculate average volume for each stock
        avg_volume_by_stock = stock_data.groupby("Stock Name")["Volume"].mean()
        average_volumes[target_date] = avg_volume_by_stock

    # Convert to DataFrame for better readability
    avg_volumes_df = pd.DataFrame(average_volumes).T
    avg_volumes_df.index.name = "Target Date"
    print(avg_volumes_df)
    return avg_volumes_df

# Function to calculate cumulative rolling 60-minute volume and find crossover timestamps
def process_intraday_data(intraday_file, avg_volumes, target_date):
    # Load intraday second-by-second data
    intraday_data = pd.read_csv(intraday_file, parse_dates=[["Date", "Time"]])
    intraday_data.rename(columns={"Date_Time": "Timestamp"}, inplace=True)

    # Filter for the specific date and trading hours (market open at 9:15 AM)
    intraday_data = intraday_data[intraday_data["Timestamp"].dt.date == target_date.date()]
    intraday_data = intraday_data[intraday_data["Timestamp"].dt.time >= pd.Timestamp("09:15:00").time()]

    # Process each stock
    results = []
    for stock in intraday_data["Stock Name"].unique():
        stock_data = intraday_data[intraday_data["Stock Name"] == stock]
        stock_data.set_index("Timestamp", inplace=True)

        # Calculate cumulative rolling 60-minute volume
        rolling_volume = stock_data["Last Traded Quantity"].rolling("60min").sum()

        # Find the first timestamp where rolling volume exceeds 30-day average
        avg_volume = avg_volumes.loc[target_date, stock]
        crossover = rolling_volume[rolling_volume > avg_volume]
        if not crossover.empty:
            first_crossover_time = crossover.index[0]
        else:
            first_crossover_time = None
        
        # Append results
        results.append({"Stock Name": stock, "Date": target_date, "First Crossover Timestamp": first_crossover_time})

    # Convert results to DataFrame
    return pd.DataFrame(results)

# Main function to integrate all processes
def main(historical_file, intraday_files):
    # Define the target dates
    target_dates = [pd.Timestamp("2024-04-19"), pd.Timestamp("2024-04-22")]

    # Step 1: Compute 30-day average volume
    avg_volumes = compute_30_day_average(historical_file, target_dates)

    # Step 2: Process intraday data for each date and find crossover timestamps
    all_results = []
    for target_date, intraday_file in zip(target_dates, intraday_files):
        results = process_intraday_data(intraday_file, avg_volumes, target_date)
        all_results.append(results)

    # Combine results for all target dates
    final_results = pd.concat(all_results, ignore_index=True)
    return final_results

if __name__ == "__main__":
    # File paths
    historical_file = "SampleDayData.csv"  # Replace with your actual file path
    intraday_files = ["19thAprilSampleData.csv", "22ndAprilSampleData.csv"]  # Replace with your intraday file paths

    # Execute the main function
    final_results = main(historical_file, intraday_files)

    # Display results and save to a CSV file
    print(final_results)
    final_results.to_csv("crossover_timestamps.csv", index=False)