import subprocess
import time
import psutil
import csv
import os
import shutil
import glob
import re

# https://realpython.com/python-sleep/ - про многопоточность и задержку времени в питоне
# https://python-scripts.com/sleep


# ======================================   Prediction   ====================================================

def get_last_file_info_prediction(folder):
    """Возвращает название и размер последнего созданного файла с расширением .json(для TinyDB)"""

    latest_file = None
    latest_time = 0

    # Проходим по всем файлам в папке и ищем последний созданный файл, соответствующий условиям
    for dirpath, _, filenames in os.walk(folder):
        for f in filenames:
            if f.startswith('Predictions_RC_') and f.endswith(".json"):
                fp = os.path.join(dirpath, f)
                file_time = os.path.getmtime(fp)  # Время последнего изменения файла
                if file_time > latest_time:
                    latest_time = file_time
                    latest_file = fp

    # Если файл найден, возвращаем его имя и размер в МБ, иначе возвращаем None
    if latest_file:
        file_size_mb = os.path.getsize(latest_file) / (1024 * 1024)  # Размер файла в МБ
        return latest_file, file_size_mb
    else:
        return None, None

def profile_process_prediction(command, folder, profile_results_path):
    """Выполняет команду и профилирует использование памяти, времени и изменения размеров файлов."""
    try:
        latest_file, file_size_mb = get_last_file_info_prediction(folder)

        # Запускаем процесс
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as p:
            start_time = time.time()
            pid = p.pid

            process = psutil.Process(pid)
            max_memory_usage = 0

            # Захватываем данные о памяти в реальном времени
            while p.poll() is None:  # Пока процесс не завершился
                try:
                    current_memory = process.memory_info().rss / (1024 * 1024)  # В МБ
                    if current_memory > max_memory_usage:
                        max_memory_usage = current_memory
                except psutil.NoSuchProcess:
                    break
                time.sleep(0.1)  # Спим между итерациями для сбора данных

            p.wait()
            end_time = time.time()

        # Запись данных в CSV файл
        with open(profile_results_path, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=' ')
            if os.stat(profile_results_path).st_size == 0:
                writer.writerow(['id',"file_name_train" ,'time_s', 'max_ram_mb', 'file_name_prediction', 'size_mb', 'command'])

            if latest_file is not None:
                writer.writerow([pid, os.path.basename(folder), end_time - start_time, max_memory_usage, ( re.sub(r"_\d{6}\.json$", "", os.path.basename(latest_file))  + "_" + os.path.basename(os.path.dirname(latest_file) )), file_size_mb, command])

    except psutil.NoSuchProcess:
        print(f"Процесс №{pid} не существует")
    except Exception as e:
        print(f"Error: {e}")


def main_prediction(mode_training):
    list_test_data_rdf = [x for x in os.listdir('/home/pustovalovatv/summer_practice/json/data/test_data') if x.endswith('.rdf')]

    list_test_data_rdf = sorted(list_test_data_rdf, key=lambda x: int(x.split('_')[1]))

    # на каждом режиме
    nm = 10

    # путь к предсказываемой тестовой выборке
    test_file_path = "/home/pustovalovatv/summer_practice/json/data/test_data_tinydb/"

    if mode_training == "merged":
        modes = ["merged"]
    else:
        modes = ["simple", "hierarchical", "frequency", "intersection", "append", 
            "top_append", "both_common_single_var1", "both_common_single_var2", "merged"]
    
    # исключительно для merged
    vars = ["priority1_maxdeep_priority2_full_minnumfg", "maxdeep_priority_full_minnumfg", "wo_notfull_priority1_maxdeep_priority2", 
        "priority_maxdeep_full_minnumfg", "maxdeep_priority_full_maxnumfg", "maxdeep_priority_full_maxnumreactionsfg"]

    profile_results_path_prediction = '/home/pustovalovatv/summer_practice/json/rcconditions_no_sql/general_profile_prediction_json.csv'

    for test_data_rdf in list_test_data_rdf[:11]:

        for mode in modes:

            deeps = [['0'], ['1'], ['2'], ['3'], ['1', '0'], ['2', '1', '0'], ['3', '2', '1', '0']]
            fg = ""
            rc_types = ['single', "common"]

            if mode == "both_common_single_var1":
                rc_types = ["common_single_v1"]
            elif mode == "both_common_single_var2":
                rc_types = ["common_single_v2"]
            elif mode == "simple":
                deeps = [['0'], ['1'], ['2'], ['3']]
            elif mode == "merged":
                fg = "-fg"
                rc_types = [["common", "single"]]
            # fg = "-fg" if mode == "merged" else ""

            for rc_type in rc_types:
                for deep in deeps:
                    list_data_sample = []
                    
                    for x in os.listdir('/home/pustovalovatv/summer_practice/json/rcconditions_no_sql/'):
                        if x.startswith(os.path.splitext(test_data_rdf)[0]):
                            tmp = x.split('_')
                            # print("mode:", mode, "tmp:", tmp, "rc_type: ", rc_type, "deep:", deep)
                            if mode == 'merged' and tmp[-3] == mode and \
                                "common,single" == tmp[-1] and tmp[-2].split(',') == deep:
                                list_data_sample.append(x)

                            elif (mode in {"both_common_single_var1", "both_common_single_var2"} and "common,single" in tmp[-1]) and \
                                tmp[-2].split(',') == deep and tmp[-3] != 'merged':
                                list_data_sample.append(x)
                            elif mode not in {"both_common_single_var1", "both_common_single_var2", "merged"}  and \
                                rc_type == tmp[-1] and tmp[-2].split(',') == deep and tmp[-3] != 'merged'  and "common,single" not in tmp[-1]:
                                list_data_sample.append(x)
                    for data_sample in list_data_sample:
            
                        command = ['python3',
                                '/home/pustovalovatv/summer_practice/json/ReactionCenterConditionPrediction/RCConditionPrediction/run_prediction.py',
                                mode, '--rc_type', rc_type, "-nm", f"{nm}", "-of", f"/home/pustovalovatv/summer_practice/json/rcconditions_no_sql/{data_sample}"]
                        
                        if mode.startswith("both_"):
                            command.append("--skip_validation")

                        command.extend(["-s", "rdf","-rdf", f"/home/pustovalovatv/summer_practice/json/data/test_data_tinydb/{test_data_rdf}", '--deep'])
                        for d in deep:
                            command.append(d)
                        if mode.startswith("both_"):

                                command.extend(["--model_file", os.path.join(f"/home/pustovalovatv/summer_practice/json/rcconditions_no_sql/{data_sample}/",  f"deep_{d}_single_rc.json"), 
                                    "--test_file", os.path.join(test_file_path, f"{os.path.splitext(test_data_rdf)[0]}.json")
                                    ])

                        else:
                            command.extend(["--model_file", os.path.join(f"/home/pustovalovatv/summer_practice/json/rcconditions_no_sql/{data_sample}/",  f"deep_{d}_{rc_type}_rc.json"), 
                                "--test_file", os.path.join(test_file_path, f"{os.path.splitext(test_data_rdf)[0]}.json")
                                ])

                        if fg:
                            for var in vars:
                                deep_name = ','.join(deep)
                                command = ['python3',
                                '/home/pustovalovatv/summer_practice/json/ReactionCenterConditionPrediction/RCConditionPrediction/run_prediction.py',
                                mode,  "-nm", f"{nm}", "-of", f"/home/pustovalovatv/summer_practice/json/rcconditions_no_sql/{data_sample}", 
                                "-s", "rdf", "-rdf", f"/home/pustovalovatv/summer_practice/json/data/test_data_tinydb/{test_data_rdf}", 
                                "--model_file", os.path.join(f"/home/pustovalovatv/summer_practice/json/rcconditions_no_sql/{data_sample}", f"Training_merged_{deep_name}.json"), 
                                "--test_file", os.path.join(test_file_path, f"{os.path.splitext(test_data_rdf)[0]}.json"), '-fg', '/home/pustovalovatv/summer_practice/json/ReactionCenterConditionPrediction/RCConditionPrediction/data/fg_queries.pkl',
                                                "-var", var, "--priority", "merged, common, single", '--rc_type', 'common', '--deep']
                                for d in deep:
                                    command.append(d)
                                try:
                                    with subprocess.Popen(command) as p:
                                        p.wait()
                                        # print(command)
                                        profile_process_prediction(command, f"/home/pustovalovatv/summer_practice/json/rcconditions_no_sql/{data_sample}", profile_results_path_prediction)
                                except:
                                    exit()
                        else:
                            try:
                                with subprocess.Popen(command) as p:

                                    p.wait()

                                    # print(command)
                                    profile_process_prediction(command, f"/home/pustovalovatv/summer_practice/json/rcconditions_no_sql/{data_sample}", profile_results_path_prediction)
                            except:
                                exit()
                        # subprocess.Popen(command)
                        # print(command)
                        # profile_process_prediction(command, f"/home/pustovalovatv/summer_practice/json/rcconditions_no_sql/{data_sample}", profile_results_path_prediction)


# =========================================   Training  =====================================================

def get_file_sizes(folder):
    latest_file = None
    latest_time = 0

    # Проходим по всем файлам в папке и ищем последний созданный файл, соответствующий условиям
    for dirpath, _, filenames in os.walk(folder):
        for f in filenames:
            if f.endswith("_rc.json") or f.startswith("Training_merged_"):
                fp = os.path.join(dirpath, f)
                file_time = os.path.getmtime(fp)  # Время последнего изменения файла
                if file_time > latest_time:
                    latest_time = file_time
                    latest_file = fp
    # Если файл найден, возвращаем его имя и размер в МБ, иначе возвращаем None
    if latest_file:
        file_size_mb = os.path.getsize(latest_file) / (1024 * 1024)  # Размер файла в МБ
        return latest_file, file_size_mb
    else:
        return None, None

def profile_process(command, folder, profile_results_path):
    try:

        # Выполнение команды и профилирование процесса
        with subprocess.Popen(command, stderr=subprocess.PIPE, stdout=subprocess.PIPE) as p:
            start_time = time.time()
            pid = p.pid

            process = psutil.Process(pid)

            # Захватываем данные о памяти в реальном времени
            max_memory_usage = 0
            while p.poll() is None:  # Пока процесс не завершился
                try:
                    current_memory = process.memory_info().rss / (1024 * 1024)  # В МБ
                    if current_memory > max_memory_usage:
                        max_memory_usage = current_memory
                except psutil.NoSuchProcess:
                    break
                time.sleep(0.1)  # Спим между итерациями для сбора данных

            p.wait()
            end_time = time.time()

        latest_file, file_size_mb = get_file_sizes(folder)

        # Запись данных в CSV файл
        with open(profile_results_path, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=' ')
            
            # Проверка на существование заголовка
            if os.stat(profile_results_path).st_size == 0:
                writer.writerow(['id', 'time_s', 'max_ram_mb', 'file_name', 'size_mb', 'command'])
            
            # Записываем данные о файлах, если они существуют
            if latest_file and file_size_mb is not None:
                writer.writerow([pid, end_time - start_time, max_memory_usage, (os.path.basename(os.path.dirname(latest_file) + "_" + os.path.basename(latest_file))), file_size_mb, command])
            else:
                print("Нет файлов")

    except psutil.NoSuchProcess:
        print(f"Процесс №{pid} не существует")
    except Exception as e:
        print(f"Error: {e}")


def main():
    deeps = [['0'], ['1'], ['2'], ['3'], ['1', '0'], ['2', '1', '0'], ['3', '2', '1', '0']]
    modes = ["general", "merged"]

    list_test_data_rdf = [x for x in os.listdir('/home/pustovalovatv/summer_practice/json/data/test_data_tinydb') if x.endswith('.rdf')]

    list_test_data_rdf = sorted(list_test_data_rdf, key=lambda x: int(x.split('_')[1]))

    # Устанавливаем путь для общего файла профилирования
    profile_results_path_training = '/home/pustovalovatv/summer_practice/json/rcconditions_no_sql/general_profile_training_json.csv'

    for test_data_rdf in list_test_data_rdf[:11]:
        my_data = os.path.splitext(test_data_rdf)[0]
        for mode in modes:
            fg = ""
            rc_types = [['single'], ["common"], ["common", "single"]]

            if mode == "merged":
                fg = "-fg"
                rc_types = [["common", "single"]]

            for rc_type in rc_types:
                for deep in deeps:
                    deep_name = ','.join(deep)
                    rc_types_name = ','.join(rc_type)
                    folder = f"/home/pustovalovatv/summer_practice/json/rcconditions_no_sql/{my_data}_model_{mode}_{deep_name}_{rc_types_name}"
                    if not os.path.isdir(folder):
                        os.mkdir(folder)
                    command = ['python3',
                               '/home/pustovalovatv/summer_practice/json/ReactionCenterConditionPrediction/RCConditionPrediction/run_training.py',
                               mode, '--deep']
                    for d in deep:
                        command.append(d)
                    command.append('--rc_type')

                    for rc in rc_type:
                        command.append(rc)

                    command.extend(['-i',
                                    f'/home/pustovalovatv/summer_practice/json/data/test_data_tinydb/{my_data}.json'])
                    command.extend(['-s', 'rdf', '-rdf', f'/home/pustovalovatv/summer_practice/json/data/test_data_tinydb/{my_data}.rdf'])
                    command.extend(['-of', folder])

                    if fg:
                        command.append('-fg')
                        command.append('/home/pustovalovatv/summer_practice/json/ReactionCenterConditionPrediction/RCConditionPrediction/data/fg_queries.pkl')

                    
                    print(command)
                    profile_process(command, folder, profile_results_path_training)
                    main_prediction(mode) 

                    # Удаляем папку после записи результатов
                    shutil.rmtree(os.path.basename(folder))

                    # Удаляем пустые логи и .pkl_chk
                    log_files = glob.glob(os.path.join("/home/pustovalovatv/summer_practice/json/rcconditions_no_sql", '*.log'))
                    pkl_chk_files = glob.glob(os.path.join("/home/pustovalovatv/summer_practice/json/rcconditions_no_sql", '*.pkl_chk'))
                    for log_file in log_files:
                        try:
                            os.remove(log_file)
                        except Exception as e:
                            print(f"Файл не удален по ошибке: {e}")

                    if pkl_chk_files:
                        for pkl_chk_file in pkl_chk_files:
                            try:
                                os.remove(pkl_chk_file)
                            except Exception as e:
                                print(f"Файл не удален по ошибке: {e}")

if __name__ == "__main__":
    main()
