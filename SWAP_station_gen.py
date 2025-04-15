# Code developed by Marina de Melo (melo.marina@usp.br) on Jan 2025, with the assistance of ChatGPT 
# Purpose: create files .met (weather files of the SWAP model) using data extracted from CLIMBra 
# for two future emission scenarios (ssp245 and ssp585) and the historical scenario (hist)

import pandas as pd
import os

# Caminhos principais
localidades_path = "D:/CLIMBra database/Gridded data/Localidades.csv"
input_directory = 'D:/CLIMBra database/Gridded data/OUTPUTS_EXTRACTED/'
output_directory = 'D:/CLIMBra database/Gridded data/OUT_SWAP_stations/'
os.makedirs(output_directory, exist_ok=True)

# Modelos climáticos e cenários de emissão
climate_models = [
    'ACCESS-ESM1-5', 'CMCC-ESM2', 'EC-EARTH3', 'INM-CM4_8', 'INM-CM5',
    'IPSL-CM6A-LR', 'MIROC6', 'MPI-ESM1-2', 'MRI-ESM2', 'NorESM2-MM'
]
emission_scenarios = ['ssp245', 'ssp585', 'hist']

# Carregar a tabela de localidades
localidades_df = pd.read_csv(localidades_path)
num_cidades = len(localidades_df)

# Colunas de interesse e mapeamento
column_mapping = {
    'City': 'station',
    'DD': 'DD',
    'MM': 'MM',
    'YYYY': 'YYYY',
    'rss_kJ/m2d': 'Rad',
    'tasmin': 'Tmin',
    'tasmax': 'Tmax',
    'hu_kPa': 'Hum',
    'sfcWind': 'Wind',
    'pr': 'Rain'
}

# Valor fixo para ETref
fixed_etref = -999.0

# Iterar sobre cada modelo climático e cenário de emissão
for model in climate_models:
    for scenario in emission_scenarios:
        
        # Caminho do arquivo de entrada
        csv_file_path = os.path.join(input_directory, f"climate_{num_cidades}_{model}_{scenario}.csv")

        # Verificar se o arquivo existe
        if not os.path.exists(csv_file_path):
            print(f"Arquivo nao encontrado: {csv_file_path}")
            continue

        # Ler o arquivo CSV
        csv_data = pd.read_csv(csv_file_path)

        # Criar diretório de saída para o modelo e cenário
        scenario_output_dir = os.path.join(output_directory, model, scenario)
        os.makedirs(scenario_output_dir, exist_ok=True)

        # Iterar sobre cada cidade única
        for city in csv_data['City'].unique():
            city_data = csv_data[csv_data['City'] == city]

            # Mapear colunas
            met_data = city_data[list(column_mapping.keys())].rename(columns=column_mapping)

            # Arredondar valores das colunas
            met_data['Rad'] = met_data['Rad'].round(1)
            met_data['Tmin'] = met_data['Tmin'].round(1)
            met_data['Tmax'] = met_data['Tmax'].round(1)
            met_data['Rain'] = met_data['Rain'].round(1)
            met_data['Wind'] = met_data['Wind'].round(1)
            met_data['Hum'] = met_data['Hum'].round(3)

            # Adicionar coluna ETref fixa
            met_data['ETref'] = fixed_etref

            # Ajustar coluna Wet baseado em Rain
            threshold = 5  # Defina o valor conforme necessário
            met_data['Wet'] = (met_data['Rain'] > 5).astype(float).round(1)

            # Ajustar o nome da cidade
            station_name = city.replace(" ", "")[:6]
            met_data['station'] = f"'{station_name}{scenario}'"

            # Salvar arquivo .met
            output_file_path = os.path.join(scenario_output_dir, f"{station_name}{scenario}.met")
            met_data.to_csv(output_file_path, index=False)

        print(f"Arquivos .met gerados em: {scenario_output_dir}")