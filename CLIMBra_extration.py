# Code developed by Marina de Melo (melo.marina@usp.br) on Jan 2025, with AI assistance
# Purpose: Extract climate data for each global circulation model considered in the CLIMBra database under each future emission scenarios (SSP245 and SSP585) and historical scenario (hist)
# Atenção: verificar viabilidade de interpolação linear dos dados climáticos para o dia 29/02 em anos bissextos!

import xarray as xr
import pandas as pd
import numpy as np

# Caminhos
localidades_path = "D:/CLIMBra database/Gridded data/Localidades.csv"
base_path = "D:/CLIMBra database/Gridded data/"
co2_base_path = "D:/CLIMBra database/Gridded data/atmospheric_{}.co2.csv"

# Lista de modelos climaticos e cenarios
modelos = [
    "ACCESS-ESM1-5", "CMCC-ESM2", "EC-EARTH3", "INM-CM4_8", "INM-CM5",
    "IPSL-CM6A-LR", "MIROC6", "MPI-ESM1-2", "MRI-ESM2", "NorESM2-MM"
]
cenarios = ["ssp245", "ssp585", "hist"]

# Variaveis a serem processadas
variaveis = ["rss", "tasmax", "tasmin", "pr", "sfcWind", "hur"]

# Anos iniciais para extracao por cenario
anos_iniciais = {
    "ssp245": "2015-01-01",
    "ssp585": "2015-01-01",
    "hist": "1980-01-01"
}

# Fator de conversao de MJ/m²/dia para kJ/m²/dia
fator_conversao_rss = 1000.

# Carregar a tabela de localidades
localidades_df = pd.read_csv(localidades_path)
num_cidades = len(localidades_df)

# Carregar dados de CO2 para cada cenario
co2_data = {}
for cenario in cenarios:
    co2_file = co2_base_path.format(cenario)
    try:
        co2_data[cenario] = pd.read_csv(co2_file)
    except FileNotFoundError:
        print(f"Arquivo de CO2 nao encontrado: {co2_file}")
        co2_data[cenario] = None

# Loop sobre modelos e cenarios
for modelo in modelos:
    for cenario in cenarios:
        print(f"Processando modelo {modelo} no cenario {cenario}...")
        nc_files_path = f"{base_path}{modelo}/{cenario}/"
        df_final = pd.DataFrame()
        ano_inicial = anos_iniciais[cenario]

        for var_name in variaveis:
            file_name = f"{modelo}-{var_name}-{cenario}.nc"
            file_path = f"{nc_files_path}{file_name}"

            try:
                dataset = xr.open_dataset(file_path).sel(time=slice(ano_inicial, None))
                resultados_var = []

                for index, row in localidades_df.iterrows():
                    lat, lon, alt, city_name = row[['LATITUDE', 'LONGITUDE', 'ELEVATION_METER', 'STATIONNAME']]
                    time_series = dataset[var_name].sel(lat=lat, lon=lon, method="nearest")

                    df_temp = pd.DataFrame({
                        'City': city_name,
                        'Latitude': lat,
                        'Longitude': lon,
                        'Altitude': alt,
                        'Date': pd.to_datetime(time_series['time'].values),
                        var_name: time_series.values
                    })

                    if var_name == "rss":
                        df_temp["rss_MJ/m2/d"] = df_temp[var_name]
                        df_temp[var_name] *= fator_conversao_rss

                    df_temp[var_name] = df_temp[var_name].interpolate(method='linear')
                    if var_name == "rss":
                        df_temp["rss_MJ/m2/d"] = df_temp["rss_MJ/m2/d"].interpolate(method='linear')

                    resultados_var.append(df_temp)

                df_var = pd.concat(resultados_var, ignore_index=True)

                if df_final.empty:
                    df_final = df_var
                else:
                    df_final = pd.merge(
                        df_final,
                        df_var,
                        on=['City', 'Latitude', 'Longitude', 'Altitude', 'Date'],
                        how='left'
                    )
                    df_final = df_final.drop_duplicates(subset=['City', 'Latitude', 'Longitude', 'Altitude', 'Date'])

            except FileNotFoundError:
                print(f"Arquivo nao encontrado: {file_path}")
                continue

        # Adicionar CO2ppm baseado no ano
        if co2_data[cenario] is not None:
            df_final['YYYY'] = df_final['Date'].dt.year
            df_final = df_final.merge(co2_data[cenario][['CO2year', 'CO2ppm']], left_on='YYYY', right_on='CO2year', how='left')
            df_final.drop(columns=['CO2year'], inplace=True)

        # Aplicacao de filtros e correcoes
        df_final.loc[df_final['hur'] > 100, 'hur'] = 99.9
        df_final.loc[df_final['sfcWind'] < 0, 'sfcWind'] = 0.5
        df_final.loc[df_final['pr'] < 0, 'pr'] = 0
        df_final.loc[df_final['rss'] < 3, 'rss'] = 3

        # Correcao para Tmin > Tmax
        mask = df_final['tasmin'] > df_final['tasmax']
        df_final.loc[mask, ['tasmin', 'tasmax']] = np.nan
        df_final[['tasmin', 'tasmax']] = df_final[['tasmin', 'tasmax']].interpolate(method='linear')

        if 'tasmax' in df_final and 'tasmin' in df_final and 'hur' in df_final:
            t_mean = (df_final['tasmax'] + df_final['tasmin']) / 2
            es = 0.6108 * np.exp((17.27 * t_mean) / (t_mean + 237.3))
            df_final['hu_kPa'] = (df_final['hur'] / 100) * es

        if 'sfcWind' in df_final:
            df_final['Wind_km/h'] = df_final['sfcWind'] * 3.6

        df_final['DD'] = df_final['Date'].dt.day
        df_final['MM'] = df_final['Date'].dt.month
        df_final['YYYY'] = df_final['Date'].dt.year
        df_final['YYYYDDD'] = df_final['Date'].apply(lambda x: f"{x.year:04d}{x.timetuple().tm_yday:03d}")

        df_final.rename(columns={'rss': 'rss_kJ/m2d', 'hur': 'hur_%', 'hu_kPa': 'hu_kPa'}, inplace=True)
        
        column_order = [
            'City', 'Latitude', 'Longitude', 'Altitude', 'DD', 'MM', 'YYYY'
        ] + [col for col in df_final.columns if col not in ['City', 'Latitude', 'Longitude', 'Altitude', 'DD', 'MM', 'YYYY', 'YYYYDDD', 'CO2ppm']] + ['YYYYDDD', 'CO2ppm']
        
        df_final = df_final[column_order]

        output_path = f"{base_path}OUTPUTS_EXTRACTED/teste/climate_{num_cidades}_{modelo}_{cenario}.csv"
        df_final.to_csv(output_path, index=False)
        print(f"Dados exportados para {output_path}.")
