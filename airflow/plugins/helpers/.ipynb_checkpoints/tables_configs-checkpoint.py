copy_tables = [
  {'name': 'immigrations',
   'key': 'immigrations',
   'file_format': 'parquet',
   'additional': ''
  },
  {'name': 'us_cities_demographics',
   'key': 'us-cities-demographics',
   'file_format': 'csv',
   'additional': " DELIMITER ';' IGNOREHEADER 1 "
  },
  {'name': 'airport_codes',
   'key': 'airport-codes',
   'file_format': 'csv',
   'additional': " DELIMITER ',' IGNOREHEADER 1 "
  },
  {'name': 'temperatures',
   'key': 'GlobalLandTemperaturesByCity',
   'file_format': 'csv',
   'additional': " DELIMITER ',' IGNOREHEADER 1 "
  },
  {'name': 'i94_countries',
   'key': 'i94_countries',
   'file_format': 'csv',
   'additional': " DELIMITER ',' IGNOREHEADER 1 "
  },
  {'name': 'i94_ports',
   'key': 'i94_ports',
   'file_format': 'csv',
   'additional': "DELIMITER ',' IGNOREHEADER 1 "
  },
  {'name': 'i94_addresses',
   'key': 'i94_addresses',
   'file_format': 'csv',
   'additional': "DELIMITER ',' IGNOREHEADER 1 "
  },
  {'name': 'i94_models',
   'key': 'i94_models',
   'file_format': 'csv',
   'additional': "DELIMITER ',' IGNOREHEADER 1 "
  },
  {'name': 'i94_visas',
   'key': 'i94_visas',
   'file_format': 'csv',
   'additional': " DELIMITER ',' IGNOREHEADER 1 "
  },
]

table_list = [
    'immigrations',
    'us_cities_demographics',
    'airport_codes',
    'temperatures',
    'i94_ports',
    'i94_countries',
    'i94_addresses',
    'i94_models',
    'i94_visas',
    'arrival_date'
]