import pandas as pd
import zipfile

denton_zips = ['76227', '75007', '75010', '76201', '76205', '76207', '76208', '76209', '76210', '75022', '75028', '76247', '75034', '76249',
               '75065', '76226', '75067', '75057', '75077', '75068', '76258', '76259', '76266', '75056', '76262']

raw_data_zip = zipfile.ZipFile('data/raw_data/raw_data_all.zip')
under150_1 = pd.read_csv(raw_data_zip.open("01 PPP sub 150k through 112420.csv"))
under150_2 = pd.read_csv(raw_data_zip.open("02 PPP sub 150k through 112420.csv"))
under150_3 = pd.read_csv(raw_data_zip.open("03 PPP sub 150k through 112420.csv"))
under150_4 = pd.read_csv(raw_data_zip.open("04 PPP sub 150k through 112420.csv"))
under150_5 = pd.read_csv(raw_data_zip.open("05 PPP sub 150k through 112420.csv"))

under150_frames = [under150_1, under150_2, under150_3, under150_4, under150_5]
under150 = pd.concat(under150_frames)
under150_DentonTX = under150.loc[(under150['State'] == 'TX') & (under150['Zip'].isin(denton_zips))]
under150.to_csv('data/under150_all.csv', header=True, compression='bz2')
under150_DentonTX.to_csv('data/under150_DentonTX.csv', header=True, compression='bz2')

over150 = pd.read_csv("data/raw_data/150k plus PPP through 112420.csv")
over150_DentonTX = over150.loc[(over150['State'] == 'TX') & (over150['Zip'].isin(denton_zips))]
over150_DentonTX.to_csv('data/over150_DentonTX.csv', header=True, compression='bz2')