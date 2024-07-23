import fastf1 as ff1
import pandas as pd
import os
from flask import Flask, jsonify

app = Flask(__name__)

# Define cache directory
cache_dir = 'cache'

# Create cache directory if it doesn't exist
if not os.path.exists(cache_dir):
    os.makedirs(cache_dir)

# Enable caching
ff1.Cache.enable_cache(cache_dir)

# Define the function to get season data
def get_season_data(year):
    if year < 2018 or year > 2024:  # Replace 2024 with the current year
        return {"error": "Year must be between 2018 and the current year"}, 400

    season_data = {
        "year": year,
        "races": [],
        "regulationChanges": [],
        "inProgress": year == 2024  # Replace 2024 with the current year
    }

    try:
        races = ff1.get_event_schedule(year, backend='fastf1')
    except Exception as e:
        app.logger.error(f"Error fetching event schedule: {e}")
        return {"error": "Failed to fetch event schedule"}, 500

    races = races.to_dict('records')

    for race in races:
        race_data = {
            "round": race['RoundNumber'],
            "result": [],
            "circuitKey": race.get('Circuit', {}).get('circuitId', 'Unknown'),  # Use 'Unknown' if Circuit data is missing
            "date": race['Session2DateUtc']
        }

        try:
            results = ff1.get_session(year, race['RoundNumber'], 'R')  # Get race results
            results.load()  # Load the data
        except Exception as e:
            app.logger.error(f"Error fetching race results: {e}")
            continue  # Skip this race and continue with the next one

        # Print the results to inspect structure
        print('Results data:', results.results)

        for driver in results.results:
            # Print driver to understand its structure
            print('Driver data:', driver)

            # Ensure driver is a dictionary before accessing its keys
            if isinstance(driver, dict):
                driver_info = {
                    "driverId": driver.get('Driver', {}).get('driverId', 'Unknown'),
                    "driverName": driver.get('Driver', {}).get('familyName', 'Unknown'),
                    "driverPhoto": "",  # Placeholder, you need to get the actual photo from another source
                    "constructorId": driver.get('Constructor', {}).get('constructorId', 'Unknown'),
                    "constructorName": driver.get('Constructor', {}).get('name', 'Unknown'),
                    "constructorPhoto": "",  # Placeholder, you need to get the actual photo from another source
                    "position": driver.get('position', 'Unknown')
                }
                race_data["result"].append(driver_info)
            else:
                print("Driver data is not a dictionary:", driver)

        season_data["races"].append(race_data)

    return season_data

# Example usage
@app.route('/season/<int:year>', methods=['GET'])
def season(year):
    season_data = get_season_data(year)
    return jsonify(season_data)

if __name__ == '__main__':
    app.run(debug=True)
