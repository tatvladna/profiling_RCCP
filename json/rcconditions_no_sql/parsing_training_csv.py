import pandas as pd

df = pd.read_csv('general_profile_training.csv', sep =" ")

# Добавление колонки "batch_name" для группировки по батчам в названии файла
df['batch_name'] = df['file_name'].apply(lambda x: 'sample_' + x.split('_')[0])

# усреднение значений "Time (s)", "Max RAM (MB)" и "Size (MB)" по batch_name
mean_values = df.groupby('batch_name').agg(
    mean_time=('time_s', 'mean'),
    mean_ram=('max_ram_mb', 'mean'),
    mean_size=('size_mb', 'mean')
).reset_index()

mean_values = mean_values.round(4)

# сохранение результатов в новый CSV файл
mean_values.to_csv('training_mean.csv', index=False)