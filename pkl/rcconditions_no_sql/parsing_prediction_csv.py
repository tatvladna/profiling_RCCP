import pandas as pd

# Загрузка данных из CSV файла
df = pd.read_csv('general_profile_prediction_pkl.csv', sep=" ")

# Добавление колонки "batch_name" для группировки по батчам в названии файла
# Название берем из file_name_train для удобства фильтрации
df['batch_name'] = df['file_name_train'].apply(lambda x: 'sample_' + x.split('_')[1])

# усреднение значений "Time (s)", "Max RAM (MB)" и "Size (MB)" по batch_name
mean_values = df.groupby('batch_name').agg(
    mean_time=('time_s', 'mean'),
    mean_ram=('max_ram_mb', 'mean'),
    mean_size_mb=('size_mb', 'mean')
).reset_index()

mean_values = mean_values.round(4)

# сохранение результатов в новый CSV файл
mean_values.to_csv('prediction_mean.csv', index=False)