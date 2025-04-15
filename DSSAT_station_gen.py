# Code developed by Marina de Melo (melo.marina@usp.br) on Jan 2025, with AI assistance
# Purpose: create files .WHT (weather files of the DSSAT model) using data extracted from CLIMBra
# for two future emission scenarios (ssp245 and ssp585) and the historical scenario (hist)

import pandas as pd
import os

# Função para criar o identificador INSI
def gerar_insi(city_name):
    partes = city_name.split()
    if len(partes) > 1:
        return f"BR{partes[0][0]}{partes[1][0]}{partes[1][1]}".upper()
    return f"BR{city_name[:3]}".upper()

# Função para criar o conteúdo do arquivo .WTH
def criar_arquivo_wth(cidade, latitude, longitude, altitude, dados, insi, cenario):
    linhas = [
        f"$WEATHER DATA : {cidade}_{cenario}",
        "@ INSI       LAT     LONG  ELEV   TAV   AMP REFHT WNDHT",
        f"  {insi}  {latitude:<7.3f}  {longitude:<8.3f}  {altitude:<5.0f} 0.0   0.0   2.0  10.0",
        "@  DATE  SRAD  TMAX  TMIN  RAIN  DEWP  WIND   PAR  RHUM    CO2"
    ]
    linhas.extend(
        f"{int(linha['YYYYDDD']):<6d}  {linha['rss_MJ/m2/d']:>4.1f}  {linha['tasmax']:>4.1f}  {linha['tasmin']:>4.1f}  {linha['pr']:>4.1f} {'':>6} {linha['Wind_km/h']:>4.1f} {'':>5}  {linha['hur_%']:>4.1f}  {linha['CO2ppm']:>4.1f}" 
        for _, linha in dados.iterrows()
    )
    return "\n".join(linhas)

# Listas de modelos e cenários
modelos = ["ACCESS-ESM1-5", "CMCC-ESM2", "EC-EARTH3", "INM-CM4_8", "INM-CM5",
           "IPSL-CM6A-LR", "MIROC6", "MPI-ESM1-2", "MRI-ESM2", "NorESM2-MM"]
cenarios = ["ssp245", "ssp585", "hist"]

# Caminhos das pastas
localidades_path = "D:/CLIMBra database/Gridded data/Localidades.csv"
input_directory = "D:/CLIMBra database/Gridded data/OUTPUTS_EXTRACTED/"
output_directory = "D:/CLIMBra database/Gridded data/OUT_DSSAT_stations/ClimaPira_Clara/"

# Garantir que a pasta de saída existe
os.makedirs(output_directory, exist_ok=True)

# Carregar a tabela de localidades
localidades_df = pd.read_csv(localidades_path)
num_cidades = len(localidades_df)

# Processar cada modelo e cenário
for modelo in modelos:
    for cenario in cenarios:
        # Construir caminho do arquivo de entrada
        input_file = os.path.join(input_directory, f"climate_{num_cidades}_{modelo}_{cenario}.csv")

        if not os.path.exists(input_file):
            print(f"Arquivo nao encontrado: {input_file}")
            continue

        # Carregar o CSV
        df = pd.read_csv(input_file)

        # Pasta de saída para o modelo e cenário
        output_model_cenario_dir = os.path.join(output_directory, modelo, cenario)
        os.makedirs(output_model_cenario_dir, exist_ok=True)

        print(f"Processado: {modelo} - {cenario}")

        # Processar dados para cada cidade
        for cidade, dados_cidade in df.groupby('City'):
            latitude = dados_cidade['Latitude'].iloc[0]
            longitude = dados_cidade['Longitude'].iloc[0]
            altitude = dados_cidade['Altitude'].iloc[0]

            # Gerar o identificador INSI
            insi = gerar_insi(cidade)

            # Nome do arquivo
            if cenario == "hist":
                nome_arquivo = f"{insi}{cenario[-2:]}80.WTH"
            else:
                nome_arquivo = f"{insi}{cenario[-2:]}15.WTH"

            # Gerar o conteúdo do arquivo .WTH
            conteudo = criar_arquivo_wth(cidade, latitude, longitude, altitude, dados_cidade, insi, cenario)

            # Caminho do arquivo de saída
            caminho_arquivo = os.path.join(output_model_cenario_dir, nome_arquivo)

            # Salvar o arquivo
            with open(caminho_arquivo, 'w') as arquivo:
                arquivo.write(conteudo)
