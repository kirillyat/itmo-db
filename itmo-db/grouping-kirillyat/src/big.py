import csv
import time
import sys
from collections import defaultdict
import heapq
import tempfile
import os
print("Current Working Directory:", os.getcwd())

import csv
import heapq

def merge_files(files, output_file, aggregation_function):

    with open(files[0], 'r') as f1, open(files[1], 'r') as f2, open(output_file, 'w') as out_file:
        csv_reader1 = csv.reader(f1)
        csv_reader2 = csv.reader(f2)
        csv_writer = csv.writer(out_file)

        row1, row2 = next(csv_reader1, None), next(csv_reader2, None)

        while row1 is not None or row2 is not None:
            if row1 is None:
                csv_writer.writerow(row2)
                row2 = next(csv_reader2, None)
            elif row2 is None:
                csv_writer.writerow(row1)
                row1 = next(csv_reader1, None)
            elif row1[0] < row2[0]:
                csv_writer.writerow(row1)
                row1 = next(csv_reader1, None)
            elif row1[0] > row2[0]:
                csv_writer.writerow(row2)
                row2 = next(csv_reader2, None)
            else:
                if aggregation_function == 'sum':
                    value = float(row1[1]) + float(row2[1])
                elif aggregation_function == 'min':
                    value = min(float(row1[1]), float(row2[1]))
                elif aggregation_function == 'max':
                    value = max(float(row1[1]), float(row2[1]))
                elif aggregation_function == 'count':
                    value = float(row1[1]) + float(row2[1])

                csv_writer.writerow([row1[0], value])
                row1, row2 = next(csv_reader1, None), next(csv_reader2, None)
                
    return



def aggregate_csv(filename, group_field, aggregation_field, aggregation_function,  output, chunk_size=50000):
    groups = defaultdict(float)
    temp_files = []

    with open(filename, "r") as file:
        reader = csv.reader(file)
        temp_dir = tempfile.mkdtemp()  # Create a temporary directory
        chunk_count = 0

        for i, row in enumerate(reader):
            group_value = int(row[group_field - 1])
            aggregation_value = float(row[aggregation_field - 1])

            if aggregation_function == "sum":
                groups[group_value] += aggregation_value
            elif aggregation_function == "max":
                groups[group_value] = max(groups[group_value], aggregation_value)
            elif aggregation_function == "min":
                groups[group_value] = min(groups[group_value], aggregation_value)
            elif aggregation_function == "count":
                groups[group_value] += 1

            if (i + 1) % chunk_size == 0:
                chunk_count += 1
                temp_file = os.path.join(temp_dir, f"temp_chunk_{chunk_count}.csv")
                print(temp_file)
                temp_files.append(temp_file)
                with open(temp_file, "w", newline="") as temp:
                    writer = csv.writer(temp)
                    for group, value in sorted(groups.items()):
                        print(group, value)
                        writer.writerow([group] + [value])
                groups.clear()

        if len(groups) > 0:
            chunk_count += 1
            temp_file = os.path.join(temp_dir, f"temp_chunk_{chunk_count}.csv")
            temp_files.append(temp_file)
            with open(temp_file, "w", newline="") as temp:
                writer = csv.writer(temp)
                for group, value in sorted(groups.items()):
                    writer.writerow([group] + [value])
                groups.clear()
    k = 0
    # Merge and aggregate temporary files
    while len(temp_files) > 1:
        temp1, temp2 = temp_files.pop(0), temp_files.pop(0)
       
        output_file = os.path.join(temp_dir, f"merged_{k}.csv")
        k+=1
        merge_files([temp1, temp2], output_file, aggregation_function)
        
        temp_files.append(output_file)

   
    with open(temp_files[0],'r') as fin, open(output,'w') as out: 
        for line in fin: 
            out.write(line)
    

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: python script.py <filename> <group_field> <aggregation_field> <aggregation_function> <output>")
        sys.exit(1)

    filename = sys.argv[1]
    group_field = int(sys.argv[2])
    aggregation_field = int(sys.argv[3])
    aggregation_function = sys.argv[4]
    if aggregation_function not in ['sum', 'min', 'max', 'count']:
        print("Неподдерживаемая агрегационная функция")
        raise
    output = sys.argv[5]

    aggregate_csv(filename, group_field, aggregation_field, aggregation_function, output)
    print("Данные сагреггированны.")
