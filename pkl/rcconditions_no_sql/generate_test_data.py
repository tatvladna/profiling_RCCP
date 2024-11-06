"python3 RCConditionPrediction/run_prediction.py merged --deep 3 2 1 0 -s rdf -rdf /data/Projects/ReactionCenterConditionPrediction/tests/test.rdf --model_file /data/Projects/ReactionCenterConditionPrediction/Training_merged_3,2,1,0.pkl --test_file /data/Projects/ReactionCenterConditionPrediction/tests/test_training_conditions.pkl -fg /data/Projects/ReactionCenterConditionPrediction/RCConditionPrediction/data/fg_queries.pkl --var maxdeep_priority_full_minnumfg"
import random
from CGRtools.files import RDFRead, RDFWrite
import os
import pickle
import subprocess


# ====================================== узнаем размер файла ===========================================
# k = 0
# with RDFRead('/home/pustovalovatv/summer_practice/pkl/data/training_set_USPTO_itog_27042022.rdf') as input_file:
#     for i, v in enumerate(input_file):
#         k += 1
#         if i % 1000 == 0: 
#             print(i)
#     # ответ: k = 336_552
#     print(k)


# 10, 100, 1000, 10_000, 100_000, all по 10 раз
sizes = [10, 100, 1000, 10_000, 100_000]

# выборок 10 по 10 реакций, 10 выборок по 100 реакций, 10 выборок по 1000 реакций и так далее

# 42, 622862, 736912, 575547, 209615, 218418, 711693, 204884, 495234, 190190
seeds = [42, 622862, 736912, 575547, 209615, 218418, 711693, 204884, 495234, 190190] # = 10 

# число k, которое посчитали
num_total_reactions = 336_552

# ============================== генерация данных .rdf ==========================================
# 10 выборок по size реакций c random.seed = seed ( 10 выборок образуется из-за len(seeds) = 10)
for size in sizes:
    for seed in seeds:
        random.seed(seed)
        r_indexes = list(range(num_total_reactions))
        sample_indexes = set(random.sample(r_indexes, size)) # size - размер выборки (10, 100, 1000...)

        with RDFRead('/home/pustovalovatv/summer_practice/pkl/data/training_set_USPTO_itog_27042022.rdf') as input_file, \
            RDFWrite(f'sample_{size}_{seed}.rdf') as output_file:
            for i, r in enumerate(input_file):
                # print(f"{i}", "sample_indexes", sample_indexes)
                
                if i in sample_indexes:
                    output_file.write(r)
                
                if i == max(sample_indexes):
                    break

# ================================ создание .pkl ===========================================
with open('/home/pustovalovatv/summer_practice/pkl/data/train_USPTO_itog_27042022.pkl', 'rb') as f:
    conditions = pickle.load(f)

list_test_data_rdf = [x for x in os.listdir('/home/pustovalovatv/summer_practice/pkl/data/test_data') if x.endswith('.rdf')]


for test_data_rdf in list_test_data_rdf:
    cgrdb_ids = []
    with RDFRead(f'/home/pustovalovatv/summer_practice/pkl/data/test_data/{test_data_rdf}') as f:
        for r in f:
                cgrdb_ids.append(r.meta['cgrdb_id']) 

    # # str
    # type(next(iter(conditions['data'].keys())))

    new_conditions = {'data': {}, 'labels': conditions['labels'], 'info': conditions['info']}
    for cgrdb_id in cgrdb_ids:
        new_conditions['data'][cgrdb_id] = conditions['data'][cgrdb_id]


    # сохраняем в pkl
    my_data = os.path.splitext(test_data_rdf)[0]
    with open(f'{my_data}.pkl', 'wb') as f:
        pickle.dump(new_conditions, f)