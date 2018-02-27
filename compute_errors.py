import os
import csv

from sets import Set
from stmoab.utils import upload_as_json, create_boto_transfer

class ComputeDAUErrors(object):
  DAU_SCHEMA = {
      "columns": [
          {
              "name": "Date",
              "type": "string",
              "friendly_name": "Date"
          }, {
              "name": "Percent Error",
              "type": "float",
              "friendly_name": "Percent Error"
          }
      ]}

  def _get_dates_and_daus(self):
    with open('event_telemetry_dau.csv', 'rb') as event_telemetry, \
         open('tiles_dau.csv', 'rb') as tiles:
      event_telemetry = csv.reader(event_telemetry)
      tiles = csv.reader(tiles) 

      dates = []
      tiles_data = []

      event_telemetry_data = [row[1] for row in event_telemetry]
      for row in tiles:
        dates.append(row[0])
        tiles_data.append(row[1])

      return dates, tiles_data, event_telemetry_data

  def get_error_json(self):
    rows = []
    percent_diffs = []
    dates, tiles_data, event_telemetry_data = self._get_dates_and_daus()

    for i in range(len(tiles_data)):
      percent_diff = abs(int(tiles_data[i]) - int(event_telemetry_data[i])) / float(tiles_data[i]) * 100
      rows.append({
          "Date": dates[i],
          "Percent Error": percent_diff
      })
      percent_diffs.append(percent_diff)

    avg_error = sum(percent_diffs) / len(percent_diffs)
    self.DAU_SCHEMA["rows"] = rows

    return avg_error

class ComputeSessionAndEventErrors(object):
  EVENT_SCHEMA = {
    "columns": [
        {
            "name": "Date",
            "type": "string",
            "friendly_name": "Date"
        }, {
            "name": "Object",
            "type": "string",
            "friendly_name": "Object"
        }, {
            "name": "Percent Error",
            "type": "float",
            "friendly_name": "Percent Error"
        }
    ]}

  def _get_structured_data(self, dataset):
    data = {}
    dates = Set()
    for row in dataset:
      date = row[0][:10]
      click_source = row[3]

      dates.add(date)
      
      if click_source not in data:
        data[click_source] = {}
      data[click_source][date] = row[1]
      
      if "session_count" not in data:
        data["session_count"] = {}
      data["session_count"][date] = row[2]

    dates = sorted(list(dates))
    return dates, data

  def _get_dates_and_data(self):
    with open('event_telemetry_events.csv', 'rb') as event_telemetry, \
         open('tiles_events.csv', 'rb') as tiles:
      event_telemetry = csv.reader(event_telemetry)
      tiles = csv.reader(tiles) 

      dates, tiles_data = self._get_structured_data(tiles)
      dates, events_data = self._get_structured_data(event_telemetry)

      return dates, tiles_data, events_data

  def get_error_json(self):
    rows = []
    avg_errors = {}

    dates, tiles_data, event_telemetry_data = self._get_dates_and_data()
    for obj in tiles_data:
      percent_diffs = []
      for date in dates[:5]:
        percent_diff = abs(int(tiles_data[obj][date]) - int(event_telemetry_data[obj][date])) / float(tiles_data[obj][date]) * 100
        rows.append({
            "Date": date,
            "Object": obj,
            "Percent Error": percent_diff
        })
        percent_diffs.append(percent_diff)

      avg_errors[obj] = sum(percent_diffs) / len(percent_diffs)

    self.EVENT_SCHEMA["rows"] = rows
    print rows
    return avg_errors

if __name__ == '__main__':
  aws_access_key = os.environ['AWS_ACCESS_KEY']
  aws_secret_key = os.environ['AWS_SECRET_KEY']
  s3_bucket_id_stats = os.environ['S3_BUCKET_ID_STATS']

  # Compute DAU Errors
  a = ComputeDAUErrors()
  avg_error = a.get_error_json()

  transfer = create_boto_transfer(aws_access_key, aws_secret_key)

  url = upload_as_json(
      "pipeline_comparison_dau",
      "activity_stream",
      transfer,
      s3_bucket_id_stats,
      a.DAU_SCHEMA
    )
  print avg_error
  print url
  
  # Compute Event Errors
  b = ComputeSessionAndEventErrors()
  avg_error = b.get_error_json()

  transfer = create_boto_transfer(aws_access_key, aws_secret_key)

  url = upload_as_json(
      "pipeline_comparison_events",
      "activity_stream",
      transfer,
      s3_bucket_id_stats,
      b.EVENT_SCHEMA
    )
  print avg_error
  print url


