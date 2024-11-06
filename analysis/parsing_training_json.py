import pandas as pd

df = pd.read_csv('general_profile_training_json.csv', sep =" ")

# Добавление колонки "batch_name" для группировки по батчам в названии файла
df['batch_name'] = df['file_name'].apply(lambda x: 'sample_' + x.split('_')[1])

# усреднение значений time_s, max_ram_mb и size_mb по batch_name
mean_values = df.groupby('batch_name').agg(
    mean_time_s=('time_s', 'mean'),
    sum_time_s= ('time_s', 'sum'),
    mean_ram_mb=('max_ram_mb', 'mean'),
    mean_size_mb=('size_mb', 'mean'),
    sum_size_mb = ('size_mb', 'sum')
).reset_index()

mean_values = mean_values.round(4)

# сохранение результатов в новый CSV файл
mean_values.to_csv('training_mean_json.csv', index=False)