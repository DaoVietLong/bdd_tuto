import pandas as pd

# Read datas from files, return a DataFrame
def read_file(filename, columns=None):
    return pd.read_csv(filename, sep='\t', header=None, names=columns)

def denormalize(avions, pilotes, vols, clients, reservations):
    # Merge datas into single DataFrame
    vols_merged = vols.merge(pilotes, left_on='PilotID', right_on='PilotID', how='left')
    vols_merged = vols_merged.merge(avions, left_on='PlaneID', right_on='PlaneID', how='left')

    vols_merged = vols_merged.merge(reservations, on='FlightID', how='left')
    vols_merged = vols_merged.merge(clients, on='ClientID', how='left')
    return vols_merged

# Namming columns in the files
avions_columns = ['PlaneID', 'PlaneName', 'Capacity', 'Airline', 'Year']
pilotes_columns = ['PilotID', 'PilotName', 'PilotExperience', 'PilotVille', 'LicenseNumber']
vols_columns = ['FlightID', 'Departure', 'Destination', 'DepDate', 'DepTime', 'ArrDate', 'ArrTime', 'PilotID', 'PlaneID']
clients_columns = ['ClientID', 'ClientName', 'ClientNumber', 'ClientAddress', 'ClientPostcode', 'ClientVille']
reservations_columns = ['ClientID', 'FlightID', 'Class', 'NumberOfSeats']

# Read datas from files
avions_data = read_file("AVIONS.txt", avions_columns)
pilotes_data = read_file("PILOTES.txt", pilotes_columns)
vols_data = read_file("VOLS.txt", vols_columns)
clients_data = read_file("CLIENTS.txt", clients_columns)
reservations_data = read_file("RESERVATIONS.txt", reservations_columns)

# Denormalize datas
denormalized_data = denormalize(avions_data, pilotes_data, vols_data, clients_data, reservations_data)

# Print datas
print(denormalized_data)

# Convert to JSON 
denormalized_data.to_json('denormalized_data.json', orient='records', lines=True)