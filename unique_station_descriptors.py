import csv

with open('run-to-descriptor.csv') as csvfile:
    # Read the scv file. will get an array of arrays.
    read_csv = csv.reader(csvfile, delimiter=',')
    # Destructuring the read_csv array to separate meta-data and data.
    meta_data, *data_matrix = read_csv
    output_matrix = []
    for data in data_matrix:
        if len(output_matrix) > 0:
            contains = False
            for out_d in output_matrix:
                # check whether output_matrix contains (station_id, station_name, parameter, program) matching row
                tuple_data = (data[1], data[2], data[3], data[4])
                tuple_out_d = (out_d[1], out_d[2], out_d[3], out_d[4])
                if tuple_data == tuple_out_d:
                    contains = True
            if not contains:
                output_matrix.append(data)
        else:
            output_matrix.append(data)

    # correct lat lon
    for data in output_matrix:
        if float(data[5]) > 10:
            data[5], data[6] = data[6], data[5]

    print("lat lon corrected. matrix-size: ", len(output_matrix))

    # update station number
    source = ['WeatherStation', 'WaterLevelGuage', 'HEC-HMS', 'SHER', 'FLO2D', 'EPM', 'MIKE11', 'WRF']
    source_range = {
        'WeatherStation': [100000, 499999],
        'WaterLevelGuage': [500000, 599999],
        'HEC-HMS': [600000, 699999],
        'SHER': [700000, 799999],
        'FLO2D': [800000, 899999],
        'EPM': [900000, 999999],
        'MIKE11': [1000000, 1099999],
        'WRF': [1100000, 1499999]
    }
    source_wise_matrix_list = {
        'WeatherStation': [],
        'WaterLevelGuage': [],
        'HEC-HMS': [],
        'SHER': [],
        'FLO2D': [],
        'EPM': [],
        'MIKE11': [],
        'WRF': []
    }

    for data in output_matrix:
        for src in source:
            if src.lower() in data[4].lower():
                source_wise_matrix_list[src].append(data)
                continue

    for key, value in source_wise_matrix_list.items():
        print('source_wise_matrix_list:', key, len(value))
        number_range = source_range[key]
        index = 0
        for data_value in value:
            data_value[0] = number_range[0] + index
            index += 1

    numbered_output_matrix = []
    for key, value in source_wise_matrix_list.items():
        for data_value in value:
            numbered_output_matrix.append(data_value)

    numbered_output_matrix.insert(0, meta_data)

with open('out.csv', 'w') as csvwritefile:
    writer = csv.writer(csvwritefile, delimiter=',')
    print(type(numbered_output_matrix), len(numbered_output_matrix))
    for line in numbered_output_matrix:
        writer.writerow(line)

print("Writing Completes")
