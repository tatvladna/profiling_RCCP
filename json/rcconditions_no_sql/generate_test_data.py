"python3 RCConditionPrediction/run_prediction.py merged --deep 3 2 1 0 -s rdf -rdf /data/Projects/ReactionCenterConditionPrediction/tests/test.rdf --model_file /data/Projects/ReactionCenterConditionPrediction/Training_merged_3,2,1,0.pkl --test_file /data/Projects/ReactionCenterConditionPrediction/tests/test_training_conditions.pkl -fg /data/Projects/ReactionCenterConditionPrediction/RCConditionPrediction/data/fg_queries.pkl --var maxdeep_priority_full_minnumfg"
import random
from CGRtools.files import RDFRead, RDFWrite
import os
import pickle
from tinydb import TinyDB, Query
import shutil


# ====================================== узнаем размер файла ===========================================
# k = 0
# with RDFRead('/home/pustovalovatv/summer_practice/data/training_set_USPTO_itog_27042022.rdf') as input_file:
#     for i, v in enumerate(input_file):
#         k += 1
#         if i % 1000 == 0: 
#             print(i)
#     # ответ: k = 336_552
#     print(k)


# # Так как .rdf-ки уже созданы, но хранятся в другой папке, то просто копируем их в другую папку
# source_folder = '/home/pustovalovatv/summer_practice/data/test_data'
# destination_folder = '/home/pustovalovatv/summer_practice/data/test_data_tinydb'

# # Создаем целевую папку, если она не существует
# os.makedirs(destination_folder, exist_ok=True)

# for file_name in os.listdir(source_folder):
#     if file_name.endswith('.rdf'):
#         # Полные пути к исходному и целевому файлам
#         source_file = os.path.join(source_folder, file_name)
#         destination_file = os.path.join(destination_folder, file_name)
        
#         # Копируем файл
#         shutil.copy2(source_file, destination_file)
#         print(f"Файл {file_name} скопирован в {destination_folder}")


# 10, 100, 1000, 10_000, 100_000, all по 10 раз
sizes = [10, 100, 1000, 10_000, 100_000]

# выборок 10 по 10 реакций, 10 выборок по 100 реакций, 10 выборок по 1000 реакций и так далее

# 42, 622862, 736912, 575547, 209615, 218418, 711693, 204884, 495234, 190190
seeds = [42, 622862, 736912, 575547, 209615, 218418, 711693, 204884, 495234, 190190] # = 10 

# число k, которое посчитали
num_total_reactions = 336_552

# ================================ создание .json ===========================================

# Загружаем исходные данные из файла .pkl
with open('/home/pustovalovatv/summer_practice/json/data/train_USPTO_itog_27042022.pkl', 'rb') as f:
    conditions = pickle.load(f)

list_test_data_rdf = [x for x in os.listdir('/home/pustovalovatv/summer_practice/json/data/test_data_tinydb') if x.endswith('.rdf')]

for test_data_rdf in list_test_data_rdf:
    cgrdb_ids = []
    with RDFRead(f'/home/pustovalovatv/summer_practice/json/data/test_data_tinydb/{test_data_rdf}') as f:
        for r in f:
            cgrdb_ids.append(r.meta['cgrdb_id'])

    # Создаем новый словарь для новых условий
    new_conditions = {'data': {}, 'labels': conditions['labels'], 'info': conditions['info']}
    for cgrdb_id in cgrdb_ids:
        if cgrdb_id in conditions['data']:
            condition_data = conditions['data'][cgrdb_id]
            if isinstance(condition_data, set):
                condition_data = list(condition_data) # json не поддерживает tuple, set
            new_conditions['data'][cgrdb_id] = condition_data

    # Определяем имя базы данных TinyDB
    db_name = os.path.splitext(test_data_rdf)[0] + '.json'
    db_path = os.path.join('/home/pustovalovatv/summer_practice/json/data/test_data_tinydb', db_name)
    
    # Сохраняем данные в TinyDB
    db = TinyDB(db_path)
    database = Query()
    db.insert({'data': new_conditions['data'], 'labels': new_conditions['labels'], 'info': new_conditions['info']})
    db.close()
